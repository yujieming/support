package com.support.meta.transport;

public interface MetaApi<K, V, R> {

    R metaKeys(MetaType type, K prefix);

    R metaList(MetaType type, K prefix);

    R put(MetaType type, K key, V value);

    R get(MetaType type, K key);

    R delete(MetaType type, K key);

}
