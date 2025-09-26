package org.multipaz.testapp

import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.hardware.usb.UsbDevice
import android.hardware.usb.UsbManager
import android.os.Build
import androidx.core.app.PendingIntentCompat
import com.acs.smartcard.Reader
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import org.multipaz.context.applicationContext
import org.multipaz.nfc.CommandApdu
import org.multipaz.nfc.NfcIsoTag
import org.multipaz.nfc.NfcTagReader
import org.multipaz.nfc.ResponseApdu
import org.multipaz.util.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume

private const val TAG = "NfcTagReaderAcs"

private class NfcIsoTagAcs(
    private val reader: Reader,
    private val slotNum: Int,
): NfcIsoTag() {
    override val maxTransceiveLength: Int
        get() = 0xfeff  // TODO

    override suspend fun transceive(command: CommandApdu): ResponseApdu {
        val recvBuffer = ByteArray(65536)
        val sendBuffer = command.encode()
        //Logger.iHex(TAG, "Sending APDU", sendBuffer)
        val responseLength = reader.transmit(
            slotNum,
            sendBuffer,
            sendBuffer.size,
            recvBuffer,
            recvBuffer.size,
        )
        val responseApduBytes = recvBuffer.sliceArray(IntRange(0, responseLength - 1))
        //Logger.iHex(TAG, "Received APDU", sendBuffer)
        return ResponseApdu.decode(responseApduBytes)
    }

    override suspend fun close() {
        reader.close()
    }

    override suspend fun updateDialogMessage(message: String) {
    }

}

class NfcTagReaderAcs(
    val usbManager: UsbManager,
    val device: UsbDevice
): NfcTagReader {
    override val external: Boolean
        get() = true

    override val dialogAlwaysShown: Boolean
        get() = false

    override suspend fun <T : Any> scan(
        message: String?,
        tagInteractionFunc: suspend (NfcIsoTag) -> T?,
        context: CoroutineContext
    ): T {
        val reader = Reader(usbManager)
        reader.open(device)
        Logger.i(TAG, "name ${reader.readerName}")


        val result = suspendCancellableCoroutine<T> { continuation ->
            var readJob: Job? = null

            reader.setOnStateChangeListener { slotNum: Int, prevState: Int, curState: Int ->
                if (slotNum != 0) {
                    return@setOnStateChangeListener
                }
                Logger.i(TAG, "onStateChanged $slotNum $curState $prevState")

                if (curState == Reader.CARD_PRESENT) {
                    Logger.i(TAG, "Woot card present in slot $slotNum")
                    reader.power(slotNum, Reader.CARD_WARM_RESET)
                    reader.setProtocol(slotNum, Reader.PROTOCOL_T0 or Reader.PROTOCOL_T1)
                    if (readJob != null) {
                        Logger.w(TAG, "job already active?")
                    }
                    readJob?.cancel()
                    readJob = CoroutineScope(context).launch {
                        val tag = NfcIsoTagAcs(reader, slotNum)
                        val funcResult = tagInteractionFunc(tag)
                        if (funcResult != null) {
                            continuation.resume(funcResult)
                        }
                    }
                }
                if (curState == Reader.CARD_ABSENT) {
                    Logger.i(TAG, "Boo card absent in slot $slotNum")
                    readJob?.cancel()
                    readJob = null
                }
            }

            continuation.invokeOnCancellation {
                Logger.i(TAG, "Was cancelled")
                readJob?.cancel()
                reader.close()
            }
        }
        //reader.close()
        Logger.i(TAG, "Am returning $result")
        return result
    }
}

suspend fun nfcTagReaderAcsCheck(): NfcTagReader? {
    try {
        val usbManager = applicationContext.getSystemService(Context.USB_SERVICE) as UsbManager
        val reader = Reader(usbManager)
        for ((deviceName, device) in usbManager.deviceList) {
            if (reader.isSupported(device)) {
                Logger.i(TAG, "Woot: $deviceName is supported")

                var flags = PendingIntent.FLAG_UPDATE_CURRENT
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
                    flags = flags or PendingIntent.FLAG_ALLOW_UNSAFE_IMPLICIT_INTENT
                }
                val pendingIntent = PendingIntentCompat.getBroadcast(
                    /* context = */ applicationContext,
                    /* requestCode = */ 0,
                    /* intent = */ Intent("com.android.example.USB_PERMISSION"),
                    /* flags = */ flags,
                    /* isMutable = */ true
                )
                usbManager.requestPermission(device, pendingIntent)
                return NfcTagReaderAcs(usbManager, device)
            }
        }

        /*
        Logger.i(TAG, "device ${reader.readerName}")

        reader.open(reader.device)
        Logger.i(TAG, "Initialized reader with name ${reader.readerName}")
         */
        return null
    } catch (e: Throwable) {
        Logger.e(TAG, "Error initializing ACS reader", e)
    }
    return null
}