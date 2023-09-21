package com.support.meta.store.rocksdb;

import com.support.meta.store.BaseStore;
import com.support.meta.store.Bucket;
import com.support.meta.store.OperateType;
import com.support.ratis.conf.StateMachineProperties;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
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

public class RocksStore extends BaseStore<ByteString, ByteString> {

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
        if (!file.exists()) {
            try {
                FileUtils.createDirectories(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            rocksDB = RocksDB.open(options, path + dir);
            RocksConfigKeys.setStore(properties, this);
        } catch (RocksDBException e) {
            LOG.error("init RocksDB error", e);
        }
    }

    @Override
    protected boolean doExist(ByteString key) {
        return rocksDB.keyMayExist(key.toByteArray(), new StringBuilder());
    }

    @Override
    protected void doPut(ByteString key, ByteString value) {
        OperateType type = OperateType.UPDATE;
        if (!exist(key)) {
            type = OperateType.CREATE;
        }
        try {
            rocksDB.put(key.toByteArray(), value.toByteArray());
        } catch (RocksDBException e) {
            LOG.error("put value error", e);
            throw new RuntimeException(e.getCause());
        }
        this.trigger(type, key, value);
    }

    @Override
    protected ByteString doGet(ByteString key) {
        try {
            byte[] bytes = rocksDB.get(key.toByteArray());
            if (Objects.isNull(bytes)) {
                return null;
            }
            return ByteString.copyFrom(bytes);
        } catch (RocksDBException e) {
            LOG.error("get value error", e);
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    protected void doDelete(ByteString key) {
        try {
            rocksDB.delete(key.toByteArray());
        } catch (RocksDBException e) {
            LOG.error("delete value error", e);
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    protected Iterator<Bucket<ByteString, ByteString>> doScan(ByteString keyPrefix) {
        return new Itr(keyPrefix);
    }

    @Override
    public void close() throws IOException {
        rocksDB.close();
    }


    private class Itr implements Iterator<Bucket<ByteString, ByteString>> {

        private RocksIterator rocksIterator;

        public Itr(ByteString keyPrefix) {
            rocksIterator = rocksDB.newIterator();
            rocksIterator.seek(keyPrefix.toByteArray());
        }


        @Override
        public boolean hasNext() {
            boolean valid = rocksIterator.isValid();
            if (valid)
                return true;
            return false;
        }

        @Override
        public Bucket<ByteString, ByteString> next() {
            byte[] keyBytes = rocksIterator.key();
            byte[] valueBytes = rocksIterator.value();
            rocksIterator.next();
            ByteString key = ByteString.copyFrom(keyBytes);
            ByteString value = ByteString.copyFrom(valueBytes);
            return new Bucket<ByteString, ByteString>(key, value);
        }
    }


    public static void main(String[] args) {
        RocksStore rocksStore = new RocksStore(new StateMachineProperties());

        rocksStore.watch(ByteString.copyFromUtf8("1"), obj -> {
            System.out.println(obj.toString());
        });

        rocksStore.watchOnce(ByteString.copyFromUtf8("2"), obj -> {
            System.out.println(obj.toString());
        });

        rocksStore.put(ByteString.copyFromUtf8("1"), ByteString.copyFromUtf8("1"));
        rocksStore.put(ByteString.copyFromUtf8("1"), ByteString.copyFromUtf8("2"));
        rocksStore.put(ByteString.copyFromUtf8("1"), ByteString.copyFromUtf8("3"));
        rocksStore.delete(ByteString.copyFromUtf8("1"));
        rocksStore.put(ByteString.copyFromUtf8("2"), ByteString.copyFromUtf8("2"));
        rocksStore.delete(ByteString.copyFromUtf8("2"));
        rocksStore.put(ByteString.copyFromUtf8("3"), ByteString.copyFromUtf8("3"));
        rocksStore.put(ByteString.copyFromUtf8("4"), ByteString.copyFromUtf8("4"));
        rocksStore.put(ByteString.copyFromUtf8("41"), ByteString.copyFromUtf8("4212"));
        rocksStore.put(ByteString.copyFromUtf8("42"), ByteString.copyFromUtf8("42113"));

        Iterator<Bucket<ByteString, ByteString>> scan = rocksStore.scan(ByteString.copyFromUtf8("4"));
        scan.forEachRemaining(stringStringBucket -> {
            System.out.println(stringStringBucket.getKey() + " --> " + stringStringBucket.getValue());
        });

    }
}
