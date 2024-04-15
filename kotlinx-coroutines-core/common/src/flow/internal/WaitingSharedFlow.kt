package kotlinx.coroutines.flow.internal

import kotlinx.coroutines.channels.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.internal.*

/**
 * Similar to [kotlinx.coroutines.flow.MutableSharedFlow], but each [emit] suspends until all collectors finished.
 *
 * @suppress **This an internal API and should not be used from general code.**
 */
@InternalCoroutinesApi
@OptIn(ExperimentalStdlibApi::class) // AutoCloseable
internal class WaitingSharedFlow<T> : Flow<T> {
    private val allChannels = mutableSetOf<Channels>()
    private val allChannelsLock = SynchronizedObject()

    override suspend fun collect(collector: FlowCollector<T>) {
        Channels().use { channels ->
            while (true) {
                collector.emit(channels.data.receive())
                channels.done.send(Unit)
            }
        }
    }

    suspend fun emit(value: T) = coroutineScope {
        val allChannels = synchronized(allChannelsLock) { allChannels.toSet() }
        for (channels in allChannels) {
            launch {
                try {
                    channels.data.send(value)
                } catch (_: ClosedSendChannelException) {
                    return@launch
                }
                try {
                    channels.done.receive()
                } catch (_: ClosedReceiveChannelException) {
                }
            }
        }
    }

    private inner class Channels(
        val data: Channel<T> = Channel(),
        val done: Channel<Unit> = Channel(),
    ) : AutoCloseable {
        init {
            synchronized(allChannelsLock) { allChannels += this }
        }

        override fun close() {
            synchronized(allChannelsLock) { allChannels -= this }
            data.close()
            done.close()
        }
    }
}
