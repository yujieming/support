package com.support.meta.store;

import java.util.Iterator;

public interface Store<K, V>  {

    boolean exist(K key);

    void put(K key, V value);

    V get(K key);

    void delete(K key);

    Iterator<Bucket<K, V>> scan(K keyPrefix);
}
