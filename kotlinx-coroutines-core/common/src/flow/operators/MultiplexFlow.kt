package kotlinx.coroutines.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.internal.*
import kotlinx.coroutines.sync.*
import kotlin.coroutines.*

/**
 * Constructs a [MultiplexFlow].
 *
 * Behavior:
 * * [getAll] is called every time the total keys collected by flows returned by [MultiplexFlow.get]
 *   changes (when collection is started or stopped).
 * * [getAll] is called with the total keys of all collected [MultiplexFlow.get] flows.
 * * [MultiplexFlow.get] calls share the data between them, such that [getAll] is not invoked when
 *   all the keys provided to [MultiplexFlow.get] are already collected by another
 *   [MultiplexFlow.get] caller.
 * * Errors in calls to [getAll] trigger a rollback to the previous keys, and collections of all
 *   [MultiplexFlow.get] with one of the new keys will throw that error.
 * * Follow-up [getAll] error, or an error after the [getAll] collection already succeeded, will
 *   clear all subscriptions and cause all [MultiplexFlow.get] collections to throw that error.
 * * If the flow returned by [getAll] finishes, all current collections of [MultiplexFlow.get]
 *   finish as well, and follow-up collections will re-invoke [getAll].
 */
public fun <K, V> MultiplexFlow(
    scope: CoroutineScope,
    getAll: suspend (keys: Set<K>) -> Flow<Map<K, V>>,
): MultiplexFlow<K, V> = MultiplexFlow(Multiplexer(getAll).launchIn(scope))

/**
 * Allows multiplexing multiple subscriptions to a single [Flow].
 *
 * This is useful when the source allows only a single subscription, but the data is needed by
 * multiple users.
 */
public class MultiplexFlow<K, V> internal constructor(private val multiplexer: Multiplexer<K, V>) {
    /**
     * Returns a [Flow] that emits [V] for the requested [K]s, based on the map provided by
     * `getAll`.
     */
    public operator fun get(vararg keys: K): Flow<V> = flow {
        val subscriptions = multiplexer.incrementUsage(*keys)
        try {
            subscriptions.filterKeys { it in keys }.values.map { it.data }.merge().collect {
                    when (it) {
                        is Multiplexer.Value -> emit(it.value)
                        is Multiplexer.Error -> throw it.error
                        is Multiplexer.Finish -> throw AbortCollection()
                    }
                }
        } catch (_: AbortCollection) {
        } finally {
            multiplexer.decrementUsage(*keys)
        }
    }

    private class AbortCollection : RuntimeException()
}

/** Internal implementation that multiplexes the data to [MultiplexFlow]. */
internal class Multiplexer<K, V>(private val getAll: suspend (keys: Set<K>) -> Flow<Map<K, V>>) {
    /** Current collected flows in [MultiplexFlow.get]. */
    private val subscriptions = MutableStateFlow(mapOf<K, DataAndUsers<V>>())

    /** Last [subscriptions] keys, to know what changed. */
    private var lastUsedKeys = setOf<K>()

    /** Last [getAll] flow processor, so we can replace it with another. */
    private var lastFlowsProcessor: Job? = null

    private val processingFlow = Mutex(false)

    /** Must only be called exactly once. */
    internal fun launchIn(scope: CoroutineScope): Multiplexer<K, V> = also {
        scope.launch {
            try {
                subscriptions.collect { current: Map<K, DataAndUsers<V>> ->
                    val usedKeys = current.usedKeys()
                    stopProcessFlow()
                    if (usedKeys.isEmpty()) {
                        lastUsedKeys = usedKeys
                        return@collect
                    }
                    val flow = tryGetAll(usedKeys) ?: return@collect
                    lastUsedKeys = usedKeys
                    // Getting succeeded, processing the flow.
                    processingFlow.lock()
                    lastFlowsProcessor = launch { flow.emitToSubscribers(usedKeys) }
                }
            } finally {
                stopProcessFlow()
                for ((data, _) in subscriptions.value.values) {
                    data.emit(Finish())
                }
            }
        }
    }

    internal suspend fun incrementUsage(vararg keys: K) = subscriptions.updateAndGet { previous ->
        previous + keys.associateWith {
            (previous[it] ?: DataAndUsers()) + currentCoroutineContext()
        }
    }

    internal suspend fun decrementUsage(vararg keys: K) {
        subscriptions.update { previous ->
            previous + keys.associateWith { previous[it]!! - currentCoroutineContext() }
        }
    }

    /** Tries [getAll], rolling back and returning `null` on failure. */
    private suspend fun tryGetAll(keys: Set<K>): Flow<Map<K, V>>? = try {
        getAll(keys)
    } catch (e: CancellationException) {
        throw e
    } catch (e: Throwable) {
        // Failed to get, rolling back.
        rollbackSubscriptions(current = keys, target = lastUsedKeys, cause = e)
        null
    }

    /** Processes the flow returned by [getAll], updating [DataAndUsers.data] of each entry. */
    private suspend fun Flow<Map<K, V>>.emitToSubscribers(keys: Set<K>) {
        catch { e ->
            // Failed to collect, cancelling everything.
            rollbackSubscriptions(current = keys, target = setOf(), cause = e)
            throw e
        }.onCompletion { e ->
                if (e != null) return@onCompletion
                // Completed normally (not cancelled or failed), finishing everything.
                for ((data, _) in subscriptions.value.values) {
                    data.emit(Finish())
                }
            }.catch {
                // Ignoring exceptions, either rolled back or finished everything.
            }.onCompletion {
                // No more changes to subscriptions data.
                processingFlow.unlock()
            }.collect { allValues: Map<K, V> ->
                for ((key, value) in allValues) {
                    if (key !in keys) continue // Ignoring keys that weren't subscribed.
                    subscriptions.value[key]!!.data.emit(Value(value))
                }
            }
    }

    private suspend fun stopProcessFlow() {
        lastFlowsProcessor?.cancel() ?: return
        processingFlow.withLock {} // Waiting for the job to finish.
    }

    /**
     * Rollbacks to [target] by removing the extras from [subscriptions] and setting the
     * [DataAndUsers.data] of the removed keys to the error provided in the [cause].
     */
    private suspend fun rollbackSubscriptions(current: Set<K>, target: Set<K>, cause: Throwable) =
    // This is an non-cancellable action, to avoid recollection cancelling to processing flow
        // that performs this.
        withContext(NonCancellable) {
            lastUsedKeys = setOf() // Preventing infinite retries, next rollback will be to empty.
            val toRemove = current - target
            // Clearing the users of the subscriptions.
            val subscriptions = subscriptions.updateAndGet { previous ->
                previous.mapValues { (key: K, dataAndUsers: DataAndUsers<V>) ->
                    if (key in toRemove) dataAndUsers.copy(users = setOf()) else dataAndUsers
                }
            }
            // THEN emitting the error, which also triggers un-subscription (which is a no-op due to
            // the above).
            for (dataType in toRemove) {
                subscriptions[dataType]!!.data.emit(Error(cause))
            }
        }

    private fun Map<K, DataAndUsers<V>>.usedKeys(): Set<K> =
        filterValues { it.users.isNotEmpty() }.keys

    internal data class DataAndUsers<V>(
        val data: WaitingSharedFlow<Emitted<V>> = WaitingSharedFlow(),
        val users: Set<CoroutineContext> = setOf(),
    ) {
        operator fun plus(user: CoroutineContext) = copy(data = data, users = users + user)

        operator fun minus(user: CoroutineContext) = copy(data = data, users = users - user)
    }

    internal sealed interface Emitted<V>

    internal data class Value<V>(val value: V) : Emitted<V>

    internal data class Error<V>(val error: Throwable) : Emitted<V>

    internal class Finish<V> : Emitted<V>
}
