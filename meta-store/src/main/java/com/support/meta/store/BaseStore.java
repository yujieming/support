package com.support.meta.store;

import java.util.Iterator;
import java.util.Objects;

public abstract class BaseStore<K, V> implements Store<K, V> {

    @Override
    public void put(K key, V value) {
        valid(key);
        valid(value);
        doPut(key, value);
    }

    protected abstract void doPut(K key, V value);

    @Override
    public V get(K key) {
        valid(key);
        return doGet(key);
    }

    protected abstract V doGet(K key);

    @Override
    public void delete(K key) {
        valid(key);
        doDelete(key);
    }

    protected abstract void doDelete(K key);

    @Override
    public Iterator<Bucket<K,V>> scan(K keyPrefix) {
        valid(keyPrefix);
        return doScan(keyPrefix);
    }

    protected abstract Iterator<Bucket<K,V>> doScan(K keyPrefix);

    @Override
    public void watch(K key, Object handler) {
        valid(key);
        doWatch(key, handler);
    }

    protected abstract void doWatch(K key, Object handler);

    protected void valid(Object value) {
        if (Objects.isNull(value))
            throw new RuntimeException("value is null");
    }


}
