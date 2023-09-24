package com.support.meta.store;

import org.apache.ratis.thirdparty.com.google.common.collect.Lists;

import java.io.IOException;
import java.text.Format;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SampleStore<V> extends BaseStore<String, V> {

    private Map<String, V> store = new ConcurrentHashMap<>(256);

    @Override
    protected boolean doExist(String storeId, String key) {
        return store.containsKey(key);
    }

    @Override
    protected void doPut(String storeId, String key, V value) {
        OperateType type = OperateType.UPDATE;
        if (!exist(storeId, key)) {
            type = OperateType.CREATE;
        }
        this.store.put(key, value);
        this.trigger(type, storeId, key, value);
    }

    @Override
    protected V doGet(String storeId, String key) {
        return this.store.get(key);
    }

    @Override
    protected void doDelete(String storeId, String key) {
        V remove = this.store.remove(key);
        this.trigger(OperateType.DELETE, storeId, key, remove);
    }

    @Override
    public Set<String> keys(String storeId) {
        return store.keySet();
    }

    @Override
    protected Iterator<Bucket<String, V>> doScan(String storeId, String keyPrefix) {
        return new Itr(keyPrefix, Collections.unmodifiableList(Lists.newArrayList(store.keySet())));
    }

    @Override
    public void close() throws IOException {
        this.store = null;
    }

    @Override
    protected String buildWatchKey(String storeId, String key) {
        if (Objects.isNull(storeId)) {
            return key;
        }
        return MessageFormat.format(FORMAT, new Object[]{storeId, key});
    }

    private class Itr implements Iterator<Bucket<String, V>> {

        private String keyPrefix;

        private List<String> keys;

        int size;
        int cursor = 0;       // index of next element to return

        public Itr(String keyPrefix, List<String> keys) {
            this.keyPrefix = keyPrefix;
            this.keys = keys;
            this.size = keys.size();
        }


        @Override
        public boolean hasNext() {
            for (; cursor < size; cursor++) {
                if (keys.get(cursor).startsWith(keyPrefix)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Bucket<String, V> next() {
            int i = cursor;
            if (i >= size)
                throw new NoSuchElementException();
            String key = keys.get(i);
            cursor = i + 1;
            return new Bucket<>(key, store.get(key));
        }
    }
}
