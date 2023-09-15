package com.support.meta.store;

import java.util.Iterator;

public interface Store<K, V> {

    void put(K key, V value);

    V get(K key);

    void delete(K key);

    Iterator<Bucket<K, V>> scan(K keyPrefix);

    void watch(K key, Object handler);
}
