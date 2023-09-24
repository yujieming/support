package com.support.meta.store;

import java.util.Iterator;
import java.util.Objects;
import java.util.Set;

public abstract class BaseStore<K, V> extends BaseWatchSupport<K, V> implements Store<K, V> {


    public BaseStore() {
    }

    public BaseStore(boolean watch) {
        super(watch);
    }

    @Override
    public boolean exist(String storeId, K key) {
        valid(key);
        return doExist(storeId, key);
    }

    protected abstract boolean doExist(String storeId, K key);

    @Override
    public void put(String storeId, K key, V value) {
        valid(key);
        valid(value);
        doPut(storeId, key, value);
    }

    protected abstract void doPut(String storeId, K key, V value);

    @Override
    public V get(String storeId, K key) {
        valid(key);
        return doGet(storeId, key);
    }

    protected abstract V doGet(String storeId, K key);

    @Override
    public void delete(String storeId, K key) {
        valid(key);
        doDelete(storeId, key);
    }

    protected abstract void doDelete(String storeId, K key);

    @Override
    public Iterator<Bucket<K, V>> scan(String storeId, K keyPrefix) {
        valid(keyPrefix);
        return doScan(storeId, keyPrefix);
    }

    protected abstract Iterator<Bucket<K, V>> doScan(String storeId, K keyPrefix);


    protected void valid(Object value) {
        if (Objects.isNull(value))
            throw new RuntimeException("value is null");
    }

}
