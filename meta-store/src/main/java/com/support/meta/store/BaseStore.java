package com.support.meta.store;

import org.rocksdb.RocksDBException;

import java.util.Iterator;
import java.util.Objects;

public abstract class BaseStore<K, V> extends BaseWatchSupport<K, V> implements Store<K, V> {


    @Override
    public boolean exist(K key) {
        valid(key);
        return doExist(key);
    }

    protected abstract boolean doExist(K key);

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
    public Iterator<Bucket<K, V>> scan(K keyPrefix) {
        valid(keyPrefix);
        return doScan(keyPrefix);
    }

    protected abstract Iterator<Bucket<K, V>> doScan(K keyPrefix);


    protected void valid(Object value) {
        if (Objects.isNull(value))
            throw new RuntimeException("value is null");
    }

}
