package com.support.meta.store;

import java.io.Closeable;
import java.util.Iterator;

public interface Store<K, V> extends Closeable {

    boolean exist(String storeId, K key);

    void put(String storeId, K key, V value);

    V get(String storeId, K key);

    void delete(String storeId, K key);

    Iterator<Bucket<K, V>> scan(String storeId, K keyPrefix);
}
