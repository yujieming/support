package com.support.meta.store;


import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.support.meta.store.Bucket.EMPTY;
import static com.support.meta.store.OperateType.TIMEOUT;

public abstract class BaseWatchSupport<K, V> implements WatchSupport<K, V> {

    private final WatchTaskContainer container;

    private boolean watch = false;

    public BaseWatchSupport() {
        this(false);
    }

    public BaseWatchSupport(boolean watch) {
        this.watch = watch;
        if (watch) {
            container = new WatchTaskContainer();
        } else {
            container = null;
        }
    }

    @Override
    public void watchOnce(String storeId, K key, WatchHandler<K, V> handler) {
        if (watch) {
            container.watch(new WatchTask(buildWatchKey(storeId, key), handler, Long.MAX_VALUE));
        }
    }


    @Override
    public void watchOnce(String storeId, K key, WatchHandler<K, V> handler, long time, TimeUnit unit) {
        if (watch) {
            long deadline = System.nanoTime() + unit.toNanos(time);
            container.watch(new WatchTask(buildWatchKey(storeId, key), handler, deadline));
        }
    }

    @Override
    public void watch(String storeId, K key, WatchHandler<K, V> handler) {
        if (watch) {
            container.watch(new WatchTask(buildWatchKey(storeId, key), handler));
        }
    }

    @Override
    public void unWatch(String storeId, K key) {
        if (watch) {
            container.unWatch(buildWatchKey(storeId, key));
        }
    }


    protected void trigger(OperateType type, String storeId, K key, V value) {
        if (watch) {
            container.trigger(type, buildWatchKey(storeId, key), value);
        }
    }

    protected abstract K buildWatchKey(String storeId, K key);

    private static class WatchTask<K, V> implements Runnable, Comparable<WatchTask<K, V>> {

        private AtomicBoolean status = new AtomicBoolean(true);

        private K key;

        private WatchHandler<K, V> watchHandler;

        private long nanoTime;

        /**
         * watchOnce
         *
         * @param key
         * @param watchHandler
         * @param nanoTime
         */
        public WatchTask(K key, WatchHandler<K, V> watchHandler, long nanoTime) {
            this.key = key;
            this.watchHandler = watchHandler;
            this.nanoTime = nanoTime;
        }

        public WatchTask(K key, WatchHandler<K, V> watchHandler) {
            this.key = key;
            this.watchHandler = watchHandler;
        }

        long getNanoTime() {
            return nanoTime;
        }

        public K getKey() {
            return key;
        }

        public void apply(OperateType type, K key, V value, boolean onlyOnce) {
            if (onlyOnce && !status.compareAndSet(true, false)) {
                throw new RuntimeException("status exception");
            }
            StoreOperate storeOperate = new StoreOperate(type, new Bucket(key, value));
            watchHandler.accept(storeOperate);
        }

        /**
         * 只执行一次的任务，执行后销毁
         */
        @Override
        public void run() {
            if (status.compareAndSet(true, false)) {
                timeOut();
            } else {
                throw new RuntimeException("status exception");
            }
        }

        private void timeOut() {
            StoreOperate storeOperate = new StoreOperate(TIMEOUT, EMPTY);
            watchHandler.accept(storeOperate);
        }

        @Override
        public int compareTo(WatchTask<K, V> o) {
            return this.getNanoTime() >= o.getNanoTime() ? 0 : 1;
        }
    }


    private static class WatchTaskContainer<K, V> implements Runnable, Closeable {

        private ConcurrentHashMap<K, Set<WatchTask<K, V>>> submit_tasks = new ConcurrentHashMap<>();

        private PriorityBlockingQueue<WatchTask<K, V>> queue = new PriorityBlockingQueue<WatchTask<K, V>>();

        private AtomicBoolean RUNNING = new AtomicBoolean(true);

        private ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("WatchTaskContainer executor Thread");
                return t;
            }
        });

        public WatchTaskContainer() {
            Executors.newSingleThreadExecutor(new ThreadFactory() {
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName("WatchTaskContainer Timeout Thread");
                    return t;
                }
            }).execute(this);
        }

        public void watch(WatchTask<K, V> task) {

            Set<WatchTask<K, V>> tasks = getOrInitSet(task.getKey());
            if (task.getNanoTime() != 0) {
                // onlyOnce
                tasks.add(task);
                enqueue(task);
            } else {
                tasks.add(task);
            }
        }

        public void unWatch(K key) {
            if (submit_tasks.containsKey(key)) {
                Set<WatchTask<K, V>> tasks = submit_tasks.get(key);
                removeFromQueue(tasks);
                submit_tasks.remove(key);
            }
        }


        public void trigger(OperateType type, K key, V value) {
            // todo 指标
            executorService.submit(() -> {
                Set<WatchTask<K, V>> tasks = submit_tasks.get(key);
                if (Objects.nonNull(tasks)) {
                    Set<WatchTask<K, V>> remove = new HashSet<>();
                    tasks.forEach(task -> {
                        boolean onlyOnce = false;
                        if (task.getNanoTime() != 0) {
                            remove.add(task);
                            onlyOnce = true;
                        }
                        task.apply(type, key, value, onlyOnce);
                    });
                    // delete
                    remove.forEach(task -> {
                        tasks.remove(task);
                    });
                    removeFromQueue(remove);
                }
            });
        }

        private Set<WatchTask<K, V>> getOrInitSet(K key) {
            if (submit_tasks.containsKey(key)) {
                return submit_tasks.get(key);
            }
            submit_tasks.put(key, new CopyOnWriteArraySet<>());
            return submit_tasks.get(key);
        }


        @Override
        public void run() {
            while (RUNNING.get()) {
                WatchTask<K, V> task = queue.peek();

                while (Objects.isNull(task)) {
                    synchronized (queue) {
                        try {
                            queue.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    task = queue.peek();
                }
                long now = System.nanoTime();
                if (task.getNanoTime() > now) {
                    synchronized (queue) {
                        long nanosTimeout = task.getNanoTime() - System.nanoTime();
                        try {
                            queue.wait(nanosTimeout);
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
                queue.notifyAll();
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

        @Override
        public void close() throws IOException {
            RUNNING.compareAndSet(true, false);
        }
    }
}
