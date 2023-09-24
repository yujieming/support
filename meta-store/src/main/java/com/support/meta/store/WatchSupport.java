package com.support.meta.store;

import java.util.concurrent.TimeUnit;

public interface WatchSupport<K, V> {
     String FORMAT = "#{0}@{1}";

    void watchOnce(String storeId, K key, WatchHandler<K, V> handler);

    void watchOnce(String storeId, K key, WatchHandler<K, V> handler, long time, TimeUnit unit);

    void watch(String storeId, K key, WatchHandler<K, V> handler);

    void unWatch(String storeId, K key);
}
