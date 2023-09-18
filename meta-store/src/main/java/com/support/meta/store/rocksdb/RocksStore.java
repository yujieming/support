package com.support.meta.store.rocksdb;

import com.support.meta.store.BaseStore;
import com.support.meta.store.Bucket;
import com.support.meta.store.OperateType;
import com.support.ratis.conf.StateMachineProperties;
import org.apache.ratis.util.FileUtils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

public class RocksStore extends BaseStore<String, String> {

    Logger LOG = LoggerFactory.getLogger(RocksStore.class);

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB rocksDB;

    private RocksStore() {
    }

    public RocksStore(StateMachineProperties properties) {
        Options options = RocksConfigKeys.rocksOptions(properties);
        String path = RocksConfigKeys.rocksPath(properties);
        String dir = RocksConfigKeys.rocksDir(properties);
        LOG.info("init rocksdb dir : " + path + dir);
        File file = new File(path + dir);
        if(!file.exists()){
            try {
                FileUtils.createDirectories(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            rocksDB = RocksDB.open(options, path + dir);
        } catch (RocksDBException e) {
            LOG.error("init RocksDB error", e);
        }
    }


    @Override
    protected boolean doExist(String key) {
        return rocksDB.keyMayExist(key.getBytes(), new StringBuilder());
    }

    @Override
    protected void doPut(String key, String value) {
        OperateType type = OperateType.UPDATE;
        if (!exist(key)) {
            type = OperateType.CREATE;
        }
        try {
            rocksDB.put(key.getBytes(), value.getBytes());
        } catch (RocksDBException e) {
            LOG.error("put value error", e);
            throw new RuntimeException(e.getCause());
        }
        this.trigger(type, key, value);
    }

    @Override
    protected String doGet(String key) {
        try {
            byte[] bytes = rocksDB.get(key.getBytes());
            if (Objects.isNull(bytes)) {
                return null;
            }
            return new String(bytes);
        } catch (RocksDBException e) {
            LOG.error("get value error", e);
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    protected void doDelete(String key) {
        try {
            rocksDB.delete(key.getBytes());
        } catch (RocksDBException e) {
            LOG.error("delete value error", e);
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    protected Iterator<Bucket<String, String>> doScan(String keyPrefix) {
        return new Itr(keyPrefix);
    }


    private class Itr implements Iterator<Bucket<String, String>> {


        private RocksIterator rocksIterator;

        public Itr(String keyPrefix) {
            rocksIterator = rocksDB.newIterator();
            rocksIterator.seek(keyPrefix.getBytes());
        }


        @Override
        public boolean hasNext() {
            boolean valid = rocksIterator.isValid();
            if (valid)
                return true;
            return false;
        }

        @Override
        public Bucket<String, String> next() {
            byte[] keyBytes = rocksIterator.key();
            byte[] valueBytes = rocksIterator.value();
            rocksIterator.next();
            String key = new String(keyBytes);
            String value = new String(valueBytes);
            return new Bucket(key, value);
        }
    }


    public static void main(String[] args) {
        RocksStore rocksStore = new RocksStore(new StateMachineProperties());

        rocksStore.watch("1", obj -> {
            System.out.println(obj.toString());
        });

        rocksStore.watchOnce("2", obj -> {
            System.out.println(obj.toString());
        });

        rocksStore.put("1", "1");
        rocksStore.put("1", "2");
        rocksStore.put("1", "3");
        rocksStore.delete("1");
        rocksStore.put("2", "2");
        rocksStore.delete("2");
        rocksStore.put("3", "3");
        rocksStore.put("4", "4");
        rocksStore.put("41", "4212");
        rocksStore.put("42", "42113");

        Iterator<Bucket<String, String>> scan = rocksStore.scan("4");
        scan.forEachRemaining(stringStringBucket -> {
            System.out.println(stringStringBucket.getKey() + " --> " + stringStringBucket.getValue());
        });

    }
}
