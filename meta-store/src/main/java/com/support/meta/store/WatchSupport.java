package com.support.meta.store;

import java.util.concurrent.TimeUnit;

public interface WatchSupport<K, V> {

    void watchOnce(K key, WatchHandler<K, V> handler);

    void watchOnce(K key, WatchHandler<K, V> handler, long time, TimeUnit unit);

    void watch(K key, WatchHandler<K, V> handler);

    void unWatch(K key);
}
