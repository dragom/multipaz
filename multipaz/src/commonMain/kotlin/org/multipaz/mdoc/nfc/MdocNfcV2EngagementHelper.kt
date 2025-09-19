package org.multipaz.mdoc.nfc

import io.ktor.utils.io.CancellationException
import kotlinx.coroutines.channels.Channel
import org.multipaz.cbor.DataItem
import org.multipaz.crypto.EcPublicKey
import org.multipaz.mdoc.connectionmethod.MdocConnectionMethod
import org.multipaz.mdoc.connectionmethod.MdocConnectionMethodBle
import org.multipaz.nfc.CommandApdu
import org.multipaz.nfc.HandoverRequestRecord
import org.multipaz.nfc.HandoverSelectRecord
import org.multipaz.nfc.NdefMessage
import org.multipaz.nfc.NdefRecord
import org.multipaz.nfc.Nfc
import org.multipaz.nfc.ResponseApdu
import org.multipaz.util.Logger
import org.multipaz.util.toHex
import kotlinx.io.bytestring.ByteString
import kotlinx.io.bytestring.ByteStringBuilder
import kotlinx.io.bytestring.append
import kotlinx.io.bytestring.encodeToByteString
import org.multipaz.cbor.Cbor
import org.multipaz.cbor.buildCborArray
import org.multipaz.mdoc.engagement.buildDeviceEngagement
import org.multipaz.mdoc.role.MdocRole
import org.multipaz.util.getUInt16

/**
 * Helper used for NFC engagement on the mdoc side.
 *
 * This implements NFC engagement v2 according to ISO/IEC 18013 Second Edition
 *
 * APDUs received from the NFC tag reader should be passed to the [processApdu] method.
 *
 * @param eDeviceKey EDeviceKey as per ISO/IEC 18013-5:2021.
 * @param onHandoverComplete the function to call when handover is complete.
 * @param onMessageReceived the function to call when a data message has been received over NFC.
 * @param onError the function to call if an error occurs.
 * @param negotiatedHandoverPicker a function to choose one of the connection methods from the mdoc reader or
 * null to not use NFC negotiated handover.
 */
class MdocNfcV2EngagementHelper(
    val eDeviceKey: EcPublicKey,
    val onHandoverComplete: (
        connectionMethods: List<MdocConnectionMethod>,
        encodedDeviceEngagement: ByteString,
        handover: DataItem) -> Unit,
    val onMessageReceived: suspend (ByteString) -> Unit,
    val onError: (error: Throwable) -> Unit,
    val negotiatedHandoverPicker: ((connectionMethods: List<MdocConnectionMethod>) -> MdocConnectionMethod)? = null,
) {
    companion object {
        private const val TAG = "MdocNfcV2EngagementHelper"
    }

    private enum class NegotiatedHandoverState {
        NOT_STARTED,
        EXPECT_HANDOVER_REQUEST_MESSAGE,
        EXPECT_PAYLOAD_MESSAGES,
    }

    private var negotiatedHandoverState = NegotiatedHandoverState.NOT_STARTED

    private var selectedFilePayload: ByteString = ByteString()
    private var inError = false

    private fun raiseError(errorMessage: String, cause: Throwable? = null) {
        inError = true
        onError(Error(errorMessage, cause))
    }

    private suspend fun processSelectApplication(command: CommandApdu): ResponseApdu {
        val requestedApplicationId = command.payload
        if (requestedApplicationId != Nfc.MDOC_NFC_ENGAGEMENT_V2_AID) {
            raiseError(
                "SelectApplication: Expected Engagement v2 AID but got " +
                        requestedApplicationId.toByteArray().toHex()
            )
            return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_FILE_OR_APPLICATION_NOT_FOUND)
        }
        negotiatedHandoverState = NegotiatedHandoverState.EXPECT_HANDOVER_REQUEST_MESSAGE
        return ResponseApdu(Nfc.RESPONSE_STATUS_SUCCESS)
    }

    private suspend fun processReadBinary(command: CommandApdu): ResponseApdu {
        val offset = command.p1 * 0x100 + command.p2
        val length = command.le
        val data = selectedFilePayload.substring(offset, offset + length)
        return ResponseApdu(Nfc.RESPONSE_STATUS_SUCCESS, data)
    }

    private var updateBinaryData: ByteStringBuilder? = null

    private suspend fun ndefTransactHandleHandoverRequest(message: NdefMessage): NdefMessage {
        // Handover Request Record must be the first record in Handover Request Message..
        val hrRecord = HandoverRequestRecord.fromNdefRecord(message.records[0])
            ?: throw Error("Handover Request Record not the first in message")
        check(hrRecord.version == 0x15) {
            "Expected Connection Handover version 1.5, got ${byteArrayOf(hrRecord.version.toByte()).toHex()}"
        }

        val availableConnectionMethods = mutableListOf<MdocConnectionMethod>()
        for (record in message.records.subList(1, message.records.size)) {
            MdocConnectionMethod.fromNdefRecord(record, MdocRole.MDOC_READER, null)?.let {
                availableConnectionMethods.add(it)
            }
        }
        if (availableConnectionMethods.isEmpty()) {
            throw Error("No supported connection methods found in Handover Request method")
        }
        val disambiguatedConnectionMethods = MdocConnectionMethod.disambiguate(
            availableConnectionMethods,
            MdocRole.MDOC
        )

        val selectedMethod = negotiatedHandoverPicker!!(disambiguatedConnectionMethods)

        // Handover Select message is defined in section 5.2 Handover Select Message
        //
        val encodedDeviceEngagement = Cbor.encode(
            buildDeviceEngagement(eDeviceKey = eDeviceKey) { }.toDataItem()
        )

        // When doing Negotiated Handover, the standard says to don't include the UUIDs in Handover Select
        // message for mdoc central client mode:
        //
        //   The following requirements apply for including the UUID field during NFC device engagement:
        //
        //     — for Negotiated Handover, if the mdoc reader supports mdoc central client mode, it shall include a
        //       UUID in the Handover Request message, to be used for mdoc central client mode;
        //     — for Negotiated Handover, if the mdoc chooses to use mdoc peripheral server mode, it shall include a
        //       UUID in the Handover Select message, to be used for mdoc peripheral server mode;
        //
        // Reference: ISO/IEC 18013-5:2021 clause 8.3.3.1.1.2 Device engagement contents
        //
        val skipUuids = selectedMethod is MdocConnectionMethodBle && selectedMethod.supportsCentralClientMode == true
        val handoverSelectMessage = generateHandoverSelectMessage(
            methods = listOf(selectedMethod),
            encodedDeviceEngagement = encodedDeviceEngagement,
            skipUuids = skipUuids,
        )

        val handover = buildCborArray {
            add(handoverSelectMessage.encode())  // Handover Select message
            add(message.encode())                // Handover Request message
        }

        negotiatedHandoverState = NegotiatedHandoverState.EXPECT_PAYLOAD_MESSAGES

        onHandoverComplete(
            listOf(selectedMethod),
            ByteString(encodedDeviceEngagement),
            handover
        )

        return handoverSelectMessage
    }

    private val queueForPayloadReply = Channel<ByteString>(Channel.UNLIMITED)

    // TODO: docs
    suspend fun sendMessage(message: ByteString) {
        Logger.i(TAG, "sendMessage: ${message.size}")
        try {
            queueForPayloadReply.send(message)
        } catch (e: Throwable) {
            Logger.w(TAG, "Ignoring failure sending message of ${message.size} over NFC", e)
        }
    }

    private suspend fun ndefTransactHandlePayloadMessages(message: NdefMessage): NdefMessage {
        val payloadMessage = message.records[0].payload
        onMessageReceived(payloadMessage)
        val responsePayloadMessage = queueForPayloadReply.receive()
        val ndefMessageToSend = NdefMessage(listOf(
            NdefRecord.createMime("application/cbor", responsePayloadMessage.toByteArray())
        ))
        return ndefMessageToSend
    }


    private fun generateHandoverSelectMessage(
        methods: List<MdocConnectionMethod>,
        encodedDeviceEngagement: ByteArray,
        skipUuids: Boolean,
    ): NdefMessage {
        val auxiliaryReferences = mutableListOf<String>("mdoc")
        val carrierConfigurationRecords = mutableListOf<NdefRecord>()
        val alternativeCarrierRecords = mutableListOf<NdefRecord>()
        for (method in methods) {
            val ndefRecordAndAlternativeCarrier = method.toNdefRecord(
                auxiliaryReferences = auxiliaryReferences,
                role = MdocRole.MDOC,
                skipUuids = skipUuids
            )!!
            carrierConfigurationRecords.add(ndefRecordAndAlternativeCarrier.first)
            alternativeCarrierRecords.add(ndefRecordAndAlternativeCarrier.second)
        }
        val handoverSelectRecord = HandoverSelectRecord(
            version = 0x15,
            embeddedMessage = NdefMessage(alternativeCarrierRecords)
        )
        return NdefMessage(
            listOf(
                handoverSelectRecord.generateNdefRecord(),
                NdefRecord(
                    tnf = NdefRecord.Tnf.EXTERNAL_TYPE,
                    type = "iso.org:18013:deviceengagement".encodeToByteString(),
                    id = "mdoc".encodeToByteString(),
                    payload = ByteString(encodedDeviceEngagement)
                )
            ) + carrierConfigurationRecords
        )
    }

    private suspend fun ndefTransact(message: NdefMessage): NdefMessage {
        return when (negotiatedHandoverState) {
            NegotiatedHandoverState.NOT_STARTED -> throw Error("Unexpected message - Negotiated Handover not started")
            NegotiatedHandoverState.EXPECT_HANDOVER_REQUEST_MESSAGE -> ndefTransactHandleHandoverRequest(message)
            NegotiatedHandoverState.EXPECT_PAYLOAD_MESSAGES -> ndefTransactHandlePayloadMessages(message)
        }
    }

    private suspend fun processUpdateBinaryNdefMessage(message: NdefMessage): ResponseApdu {
        try {
            val responseNdefMessage = ndefTransact(message)
            val responseNdefMessagePayload = responseNdefMessage.encode()
            val bsb = ByteStringBuilder()
            bsb.append((responseNdefMessagePayload.size/0x100).and(0xff).toByte())
            bsb.append(responseNdefMessagePayload.size.and(0xff).toByte())
            bsb.append(responseNdefMessagePayload)
            selectedFilePayload = bsb.toByteString()
            return ResponseApdu(Nfc.RESPONSE_STATUS_SUCCESS)
        } catch (error: Throwable) {
            raiseError(error.message!!, error)
            return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_NO_PRECISE_DIAGNOSIS)
        }
    }

    private suspend fun processUpdateBinary(command: CommandApdu): ResponseApdu {
        // This code implements the procedure specified by
        //
        //  Type 4 Tag Technical Specification Version 1.2 section 7.5.5 NDEF Write Procedure
        //
        val offset = command.p1*0x100 + command.p2
        val data = command.payload
        if (offset == 0) {
            if (data.size == 2) {
                val lenInData = data.getUInt16(0).toInt()
                if (lenInData == 0) {
                    if (updateBinaryData != null) {
                        raiseError("Got reset but is already active")
                        return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_FILE_OR_APPLICATION_NOT_FOUND)
                    }
                    updateBinaryData = ByteStringBuilder()
                } else {
                    if (updateBinaryData == null) {
                        raiseError("Got length but we are not active")
                        return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_FILE_OR_APPLICATION_NOT_FOUND)
                    }
                    if (lenInData != updateBinaryData!!.size) {
                        raiseError("Length $lenInData doesn't match received data of ${updateBinaryData!!.size} bytes")
                        return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_FILE_OR_APPLICATION_NOT_FOUND)
                    }

                    // At this point we got the whole NDEF message that the reader wanted to send.
                    val ndefMessage = NdefMessage.fromEncoded(updateBinaryData!!.toByteString().toByteArray())
                    updateBinaryData = null
                    return processUpdateBinaryNdefMessage(ndefMessage)
                }
            } else {
                if (updateBinaryData != null) {
                    raiseError("Got data in single UPDATE_BINARY but we are already active")
                    return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_FILE_OR_APPLICATION_NOT_FOUND)
                }
                val ndefMessage = NdefMessage.fromEncoded(data.toByteArray(2))
                return processUpdateBinaryNdefMessage(ndefMessage)
            }
        } else if (offset == 1) {
            raiseError("Unexpected offset $offset")
            return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_FILE_OR_APPLICATION_NOT_FOUND)
        } else {
            // offset >= 2
            if (updateBinaryData == null) {
                raiseError("Got data but we are not active")
                return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_FILE_OR_APPLICATION_NOT_FOUND)
            }
            // Writes must be sequential
            if (offset - 2 != updateBinaryData!!.size) {
                raiseError("Got data to write at offset $offset but we currently have ${updateBinaryData!!.size}")
                return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_FILE_OR_APPLICATION_NOT_FOUND)
            }
            updateBinaryData!!.append(data)
        }
        return ResponseApdu(Nfc.RESPONSE_STATUS_SUCCESS)
    }

    /**
     * Process APDUs received from the remote NFC tag reader.
     *
     * @param command The command received.
     * @return the response.
     */
    suspend fun processApdu(command: CommandApdu): ResponseApdu {
        if (inError) {
            Logger.w(TAG, "processApdu: Already in error state, responding to APDU with status 6f00")
            return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_NO_PRECISE_DIAGNOSIS)
        }
        try {
            when (command.ins) {
                Nfc.INS_SELECT -> {
                    when (command.p1) {
                        Nfc.INS_SELECT_P1_APPLICATION -> return processSelectApplication(command)
                    }
                }
                Nfc.INS_READ_BINARY -> return processReadBinary(command)
                Nfc.INS_UPDATE_BINARY -> return processUpdateBinary(command)
            }
            raiseError("Command APDU $command not supported, returning 6d00")
            return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_INSTRUCTION_NOT_SUPPORTED_OR_INVALID)
        } catch (error: Throwable) {
            raiseError("Error processing APDU: ${error.message}", error)
            return ResponseApdu(Nfc.RESPONSE_STATUS_ERROR_NO_PRECISE_DIAGNOSIS)
        }
    }

    /**
     * Must be called when the session is deactivated, e.g. when the NFC tag reader leaves the field.
     */
    fun processOnDeactivated() {
        queueForPayloadReply.cancel(CancellationException("The session was deactivated"))
    }
}

