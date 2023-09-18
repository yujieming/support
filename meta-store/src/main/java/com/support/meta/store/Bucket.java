package com.support.meta.store;

public class Bucket<K, V> {

    public static final Bucket EMPTY = new Bucket(null, null);

    private K key;
    private V value;

    public Bucket(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Bucket{" +
                "key=" + key +
                ", value=" + value +
                '}';
    }
}