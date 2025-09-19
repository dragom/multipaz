package org.multipaz.mdoc.transport

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
import org.multipaz.nfc.CommandApdu
import org.multipaz.nfc.ResponseApdu
import org.multipaz.util.Logger
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration

class NfcHybridTransportMdoc(
    private val sendMessageViaNfc: suspend (message: ByteString) -> Unit
): MdocTransport() {
    companion object {
        private const val TAG = "NfcHybridTransportMdoc"
    }

    private val _state = MutableStateFlow<State>(State.IDLE)
    override val state: StateFlow<State> = _state.asStateFlow()

    override val role: MdocRole
        get() = MdocRole.MDOC

    override val connectionMethod: MdocConnectionMethod
        get() {
            throw IllegalStateException("Should not be called")
        }

    override val scanningTime: Duration?
        get() = null

    override suspend fun advertise() {}

    private var transport: MdocTransport? = null
    private var transportMessageBacklog = mutableListOf<ByteString>()
    private var transportListenMessageJob: Job? = null

    suspend fun setTransport(transport: MdocTransport) {
        this.transport = transport
        val backlog = mutex.withLock { transportMessageBacklog.map { it.toByteArray() } }
        Logger.i(TAG, "Connected to transport, sending backlog of ${backlog.size} messages")
        for (message in backlog) {
            transport.sendMessage(message)
        }

        transportListenMessageJob = CoroutineScope(currentCoroutineContext()).launch {
            while (true) {
                val message = transport.waitForMessage()
                Logger.i(TAG, "Received message over Transport of length ${message.size}")
                conveyMessage(ByteString(message), false)
            }
        }
    }

    override suspend fun open(eSenderKey: EcPublicKey) {
        mutex.withLock {
            check(_state.value == State.IDLE) { "Expected state IDLE, got ${_state.value}" }
            _state.value = State.CONNECTED
        }
    }

    private val mutex = Mutex()
    private val writingQueue = Channel<ByteString>(Channel.UNLIMITED)
    private val incomingMessages = Channel<ByteString>(Channel.UNLIMITED)
    private var incomingMessagesNumViaNfc = 0
    private var incomingMessagesNumViaTransport = 0
    private var incomingMessagesNumConveyed = 0

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
            incomingMessages.send(message)
        } else {
            Logger.i(TAG, "Not conveying message of ${message.size} bytes received via " +
                    "$conveyanceName (already received via $otherName)")
        }
    }


    suspend fun onMessageReceivedViaNfc(message: ByteString) {
        conveyMessage(message, true)
    }

    override suspend fun sendMessage(message: ByteArray) {
        mutex.withLock {
            check(_state.value == State.CONNECTED) { "Expected state CONNECTED, got ${_state.value}" }
            Logger.iCbor(TAG, "sendMessage", message)
        }
        val messageBs = ByteString(message)
        writingQueue.send(messageBs)
        sendMessageViaNfc(messageBs)

        val toSend = mutex.withLock {
            if (transport == null) {
                transportMessageBacklog.add(messageBs)
                null
            } else {
                message
            }
        }
        toSend?.let { transport?.sendMessage(it) }
    }

    override suspend fun waitForMessage(): ByteArray {
        mutex.withLock {
            check(_state.value == State.CONNECTED) { "Expected state CONNECTED, got ${_state.value}" }
            Logger.i(TAG, "waitForMessage")
        }
        try {
            Logger.i(TAG, "Calling incomingMessages.receive()...")
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
            transportListenMessageJob?.cancel()
            transportListenMessageJob = null
            transport?.close()
            transport = null
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
        _state.value = State.FAILED
    }
}