package com.support.meta.store;

public class Bucket<K, V> {

    private K key;
    private V value;

    public Bucket(K key, V value) {
        this.key = key;
        this.value = value;
    }

    K getKey() {
        return key;
    }

    V getValue() {
        return value;
    }
}