package helder;

import haxe.ds.ReadOnlyArray;
import haxe.Constraints.IMap;
import helder.RuntimeMap;

using tink.CoreApi;

// A Function, which when given an Array of keys, returns a Promise of an Array
// of values or Errors.
typedef BatchLoadFn<K, V> = (keys: Array<K>) -> Future<Array<Outcome<V,
  Error>>>;

// Optionally turn off batching or caching or provide a cache key function or a
// custom cache instance.
typedef Options<K, V, C> = {
  final ?batch: Bool;
  final ?maxBatchSize: Int;
  final ?batchScheduleFn: (callback: () -> Void)->Void;
  final ?cache: Bool;
  final ?cacheKeyFn: (key: K) -> C;
  final ?cacheMap: RuntimeMap<C, Promise<V>>;
}

// Private: Describes a batch of requests
private typedef Batch<K, V> = {
  hasDispatched: Bool,
  keys: Array<K>,
  callbacks: Array<{
    resolve: (value: V) -> Void,
    reject: (error: Error) -> Void
  }>
}

/**
 * A `DataLoader` creates a public API for loading data from a particular
 * data back-end with unique keys such as the `id` column of a SQL table or
 * document name in a MongoDB database, given a batch loading function.
 *
 * Each `DataLoader` instance contains a unique memoized cache. Use caution when
 * used in long-lived applications or those which serve many users with
 * different access permissions and consider creating a new instance per
 * web request.
 */
class DataLoader<K, V, C> {
  final batchLoadFn: BatchLoadFn<K, V>;
  final maxBatchSize: Float;
  final batchScheduleFn: (callback: () -> Void)->Void;
  final cacheKeyFn: (key: K) -> C;
  final cache: Bool;
  final cacheMap: IMap<C, Promise<V>>;
  var batch: Batch<K, V> = null;

  public function new(batchLoadFn: BatchLoadFn<K, V>,
      ?options: Options<K, V, C>) {
    this.batchLoadFn = batchLoadFn;
    this.maxBatchSize = getValidMaxBatchSize(options);
    this.batchScheduleFn = getValidBatchScheduleFn(options);
    this.cacheKeyFn = getValidCacheKeyFn(options);
    this.cache = switch options {
      case null | {cache: null | true}: true;
      default: false;
    }
    this.cacheMap = getValidCacheMap(options);
  }

  /**
   * Loads a key, returning a `Promise` for the value represented by that key.
   */
  public function load(key: K): Promise<V> {
    var batch = getCurrentBatch();
    var cacheKey = cacheKeyFn(key);

    // If caching and there is a cache-hit, return cached Promise.
    if (cache) {
      var cachedPromise = cacheMap.get(cacheKey);
      if (cachedPromise != null) {
        return cachedPromise;
      }
    }

    // Otherwise, produce a new Promise for this key, and enqueue it to be
    // dispatched along with the current batch.
    batch.keys.push(key);
    var promise = new Promise((resolve, reject) -> {
      batch.callbacks.push({resolve: resolve, reject: reject});
    });

    // If caching, cache this promise.
    if (cache) {
      cacheMap.set(cacheKey, promise);
    }

    return promise;
  }

  // Private: Either returns the current batch, or creates and schedules a
  // dispatch of a new batch for the given loader.
  function getCurrentBatch(): Batch<K, V> {
    // If there is an existing batch which has not yet dispatched and is within
    // the limit of the batch size, then return it.
    var existingBatch = batch;
    if (existingBatch != null && !existingBatch.hasDispatched
      && existingBatch.keys.length < maxBatchSize) {
      return existingBatch;
    }

    // Otherwise, create a new batch for this loader.
    var newBatch = {hasDispatched: false, keys: [], callbacks: []};

    // Store it on the loader so it may be reused.
    batch = newBatch;

    // Then schedule a task to dispatch this batch of requests.
    batchScheduleFn(() -> {
      dispatchBatch(newBatch);
    });

    return newBatch;
  }

  function dispatchBatch(batch: Batch<K, V>) {
    // Mark this batch as having been dispatched.
    batch.hasDispatched = true;

    // If there's nothing to load, resolve any cache hits and return early.
    if (batch.keys.length == 0) {
      return;
    }

    // Call the provided batchLoadFn for this loader with the batch's keys and
    // with the loader as the `this` context.
    var batchPromise = batchLoadFn(batch.keys);

    // Assert the expected response from batchLoadFn
    if (batchPromise == null) {
      return failedDispatch(batch,
        new Error('DataLoader must be constructed with a function which accepts'
          + 'Array<key> and returns Promise<Array<value>>, but the function'
          + 'returned null'));
    }

    // Await the resolution of the call to batchLoadFn.
    batchPromise.handle(values -> {
      if (values.length != batch.keys.length) {
        throw new Error('DataLoader must be constructed with a function which accepts '
          +
          'Array<key> and returns Promise<Array<Outcome<value, Error>>>, but the function did '
          + 'not return a Promise of an Array of the same length as the Array '
          + 'of keys.'
          + '\n\nKeys:\n${batch.keys}'
          + '\n\nValues:\n${values}');
      }

      // Step through values, resolving or rejecting each Promise in the batch.
      for (i in 0...batch.callbacks.length) {
        var value = values[i];
        switch value {
          case Success(v): batch.callbacks[i].resolve(v);
          case Failure(e): batch.callbacks[i].reject(e);
        }
      }
    });
  }

  /**
   * Loads multiple keys, promising an array of values:
   *
   *     var [ a, b ] = await myLoader.loadMany([ 'a', 'b' ]);
   *
   * This is similar to the more verbose:
   *
   *     var [ a, b ] = await Promise.all([
   *       myLoader.load('a'),
   *       myLoader.load('b')
   *     ]);
   *
   * However it is different in the case where any load fails. Where
   * Promise.all() would reject, loadMany() always resolves, however each result
   * is either a value or an Error instance.
   *
   *     var [ a, b, c ] = await myLoader.loadMany([ 'a', 'b', 'badkey' ]);
   *     // c instanceof Error
   *
   */
  public function loadMany(keys: ReadOnlyArray<K>): Future<Array<Outcome<V,
    Error>>>
    return Future.ofMany([
      for (key in keys)
        load(key)
    ]);

  /**
   * Clears the value at `key` from the cache, if it exists. Returns itself for
   * method chaining.
   */
  public function clear(key: K) {
    if (cache) {
      var cacheKey = cacheKeyFn(key);
      cacheMap.remove(cacheKey);
    }
    return this;
  }

  /**
   * Clears the entire cache. To be used when some event results in unknown
   * invalidations across this particular `DataLoader`. Returns itself for
   * method chaining.
   */
  public function clearAll() {
    if (cache) {
      cacheMap.clear();
    }
    return this;
  }

  /**
   * Adds the provided key and value to the cache. If the key already
   * exists, no change is made. Returns itself for method chaining.
   *
   * To prime the cache with an error at a key, provide an Error instance.
   */
  public function prime(key: K, value: Outcome<V, Error>) {
    if (cache) {
      var cacheKey = cacheKeyFn(key);

      // Only add the key if it does not already exist.
      if (!cacheMap.exists(cacheKey)) {
        // Cache a rejected promise if the value is an Error, in order to match
        // the behavior of load(key).
        cacheMap.set(cacheKey, value);
      }
    }
    return this;
  }

  // Private: do not cache individual loads if the entire batch dispatch fails,
  // but still reject each request so they do not hang.
  function failedDispatch(batch: Batch<K, V>, error: Error) {
    for (i in 0...batch.keys.length) {
      clear(batch.keys[i]);
      batch.callbacks[i].reject(error);
    }
  }

  // Private: given the DataLoader's options, produce a valid max batch size.
  function getValidMaxBatchSize(?options: Options<K, V, C>): Float
    return switch options {
      case null | {batch: null | true, maxBatchSize: null}:
        Math.POSITIVE_INFINITY;
      case {batch: null | true, maxBatchSize: maxBatchSize}:
        if (maxBatchSize < 1)
          throw new Error('maxBatchSize must be a positive number: ${maxBatchSize}');
        maxBatchSize;
      default: 1;
    }

  // Private
  function getValidBatchScheduleFn(?options: Options<K, V, C>): (() -> Void)->
    Void
    return switch options {
      case null | {batchScheduleFn: null}: f -> haxe.MainLoop.add(f);
      case {batchScheduleFn: f}: f;
    }

  // Private: given the DataLoader's options, produce a cache key function.
  function getValidCacheKeyFn(?options: Options<K, V, C>): K->C
    return switch options {
      case null | {cacheKeyFn: null}: (k: K) -> (cast k : C);
      case {cacheKeyFn: f}: f;
    }

  // Private: given the DataLoader's options, produce a CacheMap to be used.
  static function getValidCacheMap<K, V, C>(?options: Options<K, V,
    C>): Null<IMap<C, Promise<V>>> {
    return switch options {
      case null | {cacheMap: null}: new RuntimeMap();
      case {cacheMap: cacheMap}: cacheMap;
    }
  }
}
