package org.multipaz.mdoc.transport

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.io.bytestring.ByteString
import org.multipaz.crypto.EcPublicKey
import org.multipaz.mdoc.connectionmethod.MdocConnectionMethod
import org.multipaz.mdoc.role.MdocRole
import org.multipaz.nfc.NdefMessage
import org.multipaz.nfc.NdefRecord
import org.multipaz.nfc.NfcIsoTag
import org.multipaz.util.Logger
import kotlin.time.Duration

/**
 * A hybrid transport for ISO 18013-5 Second Edition NFCv2 engagement on the reader side.
 *
 * @param nfcTag the [NfcIsoTag] that was used for NFCv2 engagement and which is used to send data over.
 * @param negotiatedTransport the [MdocTransport] that was negotiated between reader and holder.
 */
class NfcHybridTransportMdocReader(
    private val nfcTag: NfcIsoTag,
    private val negotiatedTransport: MdocTransport
): MdocTransport() {
    companion object {
        private const val TAG = "NfcHybridTransportMdocReader"
    }

    /**
     * The number of messages sent via NFC.
     */
    val numMessagesSentViaNfc: Int
        get() = sentMessagesNumViaNfc

    /**
     * The number of messages received via NFC before receiving they were received using [negotiatedTransport].
     */
    val numMessagesReceivedViaNfc: Int
        get() = incomingMessagesNumConveyedWasViaNfc

    private val _state = MutableStateFlow<State>(State.IDLE)
    override val state: StateFlow<State> = _state.asStateFlow()

    override val role: MdocRole
        get() = MdocRole.MDOC_READER

    override val connectionMethod: MdocConnectionMethod
        get() = negotiatedTransport.connectionMethod

    override val scanningTime: Duration?
        get() = null

    override suspend fun advertise() {
    }

    private val mutex = Mutex()
    private var ioJob: Job? = null
    private val writingQueue = Channel<ByteString>(Channel.UNLIMITED)
    private val incomingMessages = Channel<ByteString>(Channel.UNLIMITED)

    // Counts how many messages was sent via NFC
    private var sentMessagesNumViaNfc: Int = 0

    // Counts how many total messages we received via NFC
    private var incomingMessagesNumViaNfc = 0

    // Counts how many total messages we received via Transport
    private var incomingMessagesNumViaTransport = 0

    // Counts how many total messages received via either NFC or Transport
    private var incomingMessagesNumConveyed = 0

    // Number of messages where we received them first on NFC
    private var incomingMessagesNumConveyedWasViaNfc = 0

    // Number of messages where we received them first on Transport
    private var incomingMessagesNumConveyedWasViaTransport = 0

    private var nfcNotAvailable = false

    private var negotiatedTransportOpenJob: Job? = null
    private val negotiatedTransportPendingOutgoingMessages = mutableListOf<ByteString>()

    private suspend fun conveyMessage(message: ByteString, fromNfc: Boolean) {
        val shouldConveyMessage = mutex.withLock {
            incomingMessagesNumConveyed += 1
            if (fromNfc) {
                incomingMessagesNumViaNfc += 1
                incomingMessagesNumViaNfc == incomingMessagesNumConveyed
            } else {
                incomingMessagesNumViaTransport += 1
                incomingMessagesNumViaTransport == incomingMessagesNumConveyed
            }
        }
        val (conveyanceName, otherName) = if (fromNfc) Pair("NFC", "Transport") else Pair("Transport", "NFC")
        if (shouldConveyMessage) {
            Logger.i(TAG, "Conveying message of ${message.size} bytes received via " +
                    "$conveyanceName (not yet received via $otherName)")
            if (fromNfc) {
                incomingMessagesNumConveyedWasViaNfc += 1
            }
            incomingMessages.send(message)
        } else {
            Logger.i(TAG, "Not conveying message of ${message.size} bytes received via " +
                    "$conveyanceName (already received via $otherName)")
        }
    }


    override suspend fun open(eSenderKey: EcPublicKey) {
        mutex.withLock {
            check(_state.value == State.IDLE) { "Expected state IDLE, got ${_state.value}" }
            _state.value = State.CONNECTED
        }

        negotiatedTransportOpenJob = CoroutineScope(currentCoroutineContext()).launch {
            try {
                negotiatedTransport.open(eSenderKey)
                negotiatedTransportOpenJob = null
                Logger.i(TAG, "Connected to negotiated transport, sending " +
                        "${negotiatedTransportPendingOutgoingMessages.size} pending messages")
                for (message in negotiatedTransportPendingOutgoingMessages) {
                    negotiatedTransport.sendMessage(message.toByteArray())
                }

                while (true) {
                    val message = negotiatedTransport.waitForMessage()
                    Logger.i(TAG, "Received message over Transport of length ${message.size}")
                    conveyMessage(ByteString(message), false)
                }
            } catch (e: Throwable) {
                mutex.withLock {
                    failTransport(e)
                }
            }
        }

        ioJob = CoroutineScope(currentCoroutineContext()).launch {
            try {
                while (true) {
                    val messageToSend = writingQueue.receive()
                    val ndefMessageToSend = NdefMessage(listOf(
                        NdefRecord.createMime("application/cbor", messageToSend.toByteArray())
                    ))
                    Logger.i(TAG, "Sending message over NFC of length ${messageToSend.size}")
                    val responseNdefMessage = nfcTag.ndefTransact(
                        ndefMessage = ndefMessageToSend,
                        wtInt = 15,
                        nWait = 15,
                        onMessageSent = { sentMessagesNumViaNfc += 1 }
                    )
                    val responseMessage = responseNdefMessage.records[0].payload
                    Logger.i(TAG, "Received message over NFC of length ${responseMessage.size}")
                    conveyMessage(responseMessage, true)
                }
            } catch (e: Throwable) {
                Logger.w(TAG, "Error while transacting on NFC - continuing on non-NFC transport", e)
                nfcNotAvailable = true
            }
        }
    }

    override suspend fun sendMessage(message: ByteArray) {
        mutex.withLock {
            check(_state.value == State.CONNECTED) { "Expected state CONNECTED, got ${_state.value}" }
            Logger.i(TAG, "sendMessage")
        }
        if (!nfcNotAvailable) {
            writingQueue.send(ByteString(message))
        }

        if (negotiatedTransport.state.value == State.CONNECTED) {
            try {
                negotiatedTransport.sendMessage(message)
            } catch (e: Throwable) {
                mutex.withLock {
                    failTransport(e)
                }
                throw e
            }
        } else {
            negotiatedTransportPendingOutgoingMessages.add(ByteString(message))
        }
    }

    override suspend fun waitForMessage(): ByteArray {
        mutex.withLock {
            check(_state.value == State.CONNECTED) { "Expected state CONNECTED, got ${_state.value}" }
            Logger.i(TAG, "waitForMessage")
        }
        try {
            return incomingMessages.receive().toByteArray()
        } catch (error: CancellationException) {
            throw error
        } catch (error: Throwable) {
            if (_state.value == State.CLOSED) {
                throw MdocTransportClosedException("Transport was closed while waiting for message")
            } else {
                mutex.withLock {
                    failTransport(error)
                }
                throw MdocTransportException("Failed while waiting for message", error)
            }
        }
    }

    override suspend fun close() {
        mutex.withLock {
            if (_state.value == State.CLOSED) {
                return
            }
            Logger.i(TAG, "close")
            incomingMessages.close()
            ioJob?.cancel()
            ioJob = null
            incomingMessages.close()
            nfcTag.close()
            negotiatedTransportOpenJob?.cancel()
            negotiatedTransportOpenJob = null
            negotiatedTransport.close()
            if (_state.value != State.FAILED) {
                _state.value = State.CLOSED
            }
        }
    }

    private var inError = false

    private fun failTransport(error: Throwable) {
        check(mutex.isLocked) { "failTransport called without holding lock" }
        inError = true
        if (_state.value == State.FAILED || _state.value == State.CLOSED) {
            return
        }
        Logger.w(TAG, "Failing transport with error", error)
        Error().printStackTrace()
        incomingMessages.close(error)
        ioJob?.cancel()
        ioJob = null
        incomingMessages.close()
        // TODO: need to also do something with nfcTag and negotiatedTransport...
        _state.value = State.FAILED
    }
}