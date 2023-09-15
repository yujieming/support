package com.support.meta.store;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class BaseWatchSupport<K, V> implements WatchSupport<K, V>, Runnable {

    private ConcurrentHashMap<K, Set<WatchTask<K, V>>> submit_tasks = new ConcurrentHashMap<>();

    private PriorityBlockingQueue<WatchTask<K, V>> queue = new PriorityBlockingQueue<WatchTask<K, V>>();

    private AtomicBoolean RUNNING = new AtomicBoolean(true);

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
        if (submit_tasks.containsKey(key)) {
            Set<WatchTask<K, V>> watchTasks = submit_tasks.get(key);
            removeFromQueue(watchTasks);
        }
    }

    public void run() {

        while (RUNNING.get()) {
            WatchTask task = queue.peek();
            long now = System.nanoTime();
            if (task.getNanoTime() > now) {
                synchronized (queue) {
                    long nanosTimeout = task.getNanoTime() - System.nanoTime();
                    try {
                        wait(nanosTimeout);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            } else {
                task = queue.poll();
                task.run();
            }
        }
    }

    private void enqueue(WatchTask task) {
        synchronized (queue) {
            queue.add(task);
            notifyAll();
        }
    }

    private void removeFromQueue(Set<WatchTask<K, V>> tasks) {
        if (queue.isEmpty()) {
            return;
        }
        tasks.forEach(task -> {
            if (queue.contains(task)) {
                queue.remove(task);
            }
        });
    }

    static class WatchTask<K, V> implements Runnable {

        private WatchHandler<K, V> watchHandler;
        private long nanoTime;

        long getNanoTime() {
            return nanoTime;
        }

        public void run() {
        }
    }
}
