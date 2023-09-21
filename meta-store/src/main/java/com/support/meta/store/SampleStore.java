package com.support.meta.store;

import org.apache.ratis.thirdparty.com.google.common.collect.Lists;
import org.apache.ratis.thirdparty.io.grpc.internal.JsonUtil;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class SampleStore<V> extends BaseStore<String, V> {

    private Map<String, V> store = new ConcurrentHashMap<>(256);

    @Override
    protected boolean doExist(String key) {
        return store.containsKey(key);
    }

    @Override
    protected void doPut(String key, V value) {
        OperateType type = OperateType.UPDATE;
        if (!exist(key)) {
            type = OperateType.CREATE;
        }
        this.store.put(key, value);
        this.trigger(type, key, value);
    }

    @Override
    protected V doGet(String key) {
        return this.store.get(key);
    }

    @Override
    protected void doDelete(String key) {
        V remove = this.store.remove(key);
        this.trigger(OperateType.DELETE, key, remove);
    }

    @Override
    protected Iterator<Bucket<String, V>> doScan(String keyPrefix) {
        return new Itr(keyPrefix, Collections.unmodifiableList(Lists.newArrayList(store.keySet())));
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
