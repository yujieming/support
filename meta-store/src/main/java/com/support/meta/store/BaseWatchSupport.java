package com.support.meta.store;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BaseWatchSupport<K, V> implements WatchSupport<K, V> {

    private PriorityBlockingQueue<WatchTask> queue = new PriorityBlockingQueue<WatchTask>();


    @Override
    public void watchOnce(K key, WatchHandler<K, V> handler) {

    }

    @Override
    public void watchOnce(K key, WatchHandler<K, V> handler, long time, TimeUnit unit) {

    }

    @Override
    public void watch(K key, WatchHandler<K, V> handler) {

    }

    @Override
    public void unWatch(K key) {

    }

    class WatchTask {

    }
}
