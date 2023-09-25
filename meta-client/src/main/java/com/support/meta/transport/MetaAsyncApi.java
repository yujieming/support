package com.support.meta.transport;

import java.util.concurrent.CompletableFuture;

public interface MetaAsyncApi<K, V, R> {

    CompletableFuture<R> metaKeys(MetaType type, K prefix);

    CompletableFuture<R> metaList(MetaType type, K prefix);

    CompletableFuture<R> put(MetaType type, K key, V value);

    CompletableFuture<R> get(MetaType type, K key);

    CompletableFuture<R> delete(MetaType type, K key);

}
