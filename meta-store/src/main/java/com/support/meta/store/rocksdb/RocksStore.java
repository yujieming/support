package com.support.meta.store.rocksdb;

import com.support.meta.store.BaseStore;
import com.support.meta.store.Bucket;
import com.support.meta.store.OperateType;
import com.support.ratis.conf.StateMachineProperties;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.FileUtils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RocksStore extends BaseStore<ByteString, ByteString> {

    public static final ByteString SCAN_ALL = ByteString.copyFromUtf8("*");

    Logger LOG = LoggerFactory.getLogger(RocksStore.class);

    private ConcurrentHashMap<String, ColumnFamilyHandle> columnFamilies = new ConcurrentHashMap<>();

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB rocksDB;

    private RocksStore() {
    }

    @Override
    protected ByteString buildWatchKey(String storeId, ByteString key) {
        if (Objects.isNull(storeId)) {
            return key;
        }
        String format = MessageFormat.format(FORMAT, new Object[]{storeId, key});
        return ByteString.copyFromUtf8(format);
    }

    public RocksStore(StateMachineProperties properties) {
        super(RocksConfigKeys.watch(properties));
        Options options = RocksConfigKeys.rocksOptions(properties);
        String path = RocksConfigKeys.rocksPath(properties);
        String dir = RocksConfigKeys.rocksDir(properties);
        String rocksPath = path + File.separator + dir;
        LOG.info("init rocksdb dir : " + rocksPath);
        File file = new File(rocksPath);
        if (!file.exists()) {
            try {
                FileUtils.createDirectories(file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            rocksDB = RocksDB.open(options, rocksPath);
            RocksConfigKeys.setStore(properties, this);
        } catch (RocksDBException e) {
            LOG.error("init RocksDB error", e);
        }
    }

    @Override
    protected boolean doExist(String storeId, ByteString key) {
        return rocksDB.keyMayExist(getOrCreateColumnFamily(storeId), key.toByteArray(), new StringBuilder());
    }

    @Override
    protected void doPut(String storeId, ByteString key, ByteString value) {
        OperateType type = OperateType.UPDATE;
        if (!exist(storeId, key)) {
            type = OperateType.CREATE;
        }
        try {
            rocksDB.put(getOrCreateColumnFamily(storeId), key.toByteArray(), value.toByteArray());
        } catch (RocksDBException e) {
            LOG.error("put value error", e);
            throw new RuntimeException(e.getCause());
        }
        this.trigger(type, storeId, key, value);
    }

    @Override
    protected ByteString doGet(String storeId, ByteString key) {
        try {
            byte[] bytes = rocksDB.get(getOrCreateColumnFamily(storeId), key.toByteArray());
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
    protected void doDelete(String storeId, ByteString key) {
        try {
            rocksDB.delete(getOrCreateColumnFamily(storeId), key.toByteArray());
        } catch (RocksDBException e) {
            LOG.error("delete value error", e);
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public Set<ByteString> keys(String storeId) {
        ColumnFamilyHandle columnFamily = getOrCreateColumnFamily(storeId);
        RocksIterator rocksIterator = rocksDB.newIterator(columnFamily);
        rocksIterator.seekToFirst();
        HashSet<ByteString> keys = new HashSet<>();
        while (rocksIterator.isValid()) {
            keys.add(ByteString.copyFrom(rocksIterator.key()));
            rocksIterator.next();
        }

        return keys;
    }

    @Override
    protected Iterator<Bucket<ByteString, ByteString>> doScan(String storeId, ByteString keyPrefix) {
        return new Itr(storeId, keyPrefix);
    }


    @Override
    public void close() throws IOException {
        rocksDB.close();
    }

    private ColumnFamilyHandle getOrCreateColumnFamily(String kvStoreId) {
        if (Objects.isNull(kvStoreId)) {
            return rocksDB.getDefaultColumnFamily();
        }
        ColumnFamilyHandle columnFamily = columnFamilies.get(kvStoreId);
        if (columnFamily == null) {
            ColumnFamilyOptions columnOptions = getColumnFamilyOptions(kvStoreId);
            ColumnFamilyDescriptor columnDescriptor =
                    new ColumnFamilyDescriptor(kvStoreId.getBytes(), columnOptions);
            try {
                columnFamily = rocksDB.createColumnFamily(columnDescriptor);
                columnFamilies.put(kvStoreId, columnFamily);
            } catch (RocksDBException e) {
                throw new RuntimeException("Error creating ColumnFamilyHandle.", e);
            }
        }
        return columnFamily;
    }

    private ColumnFamilyOptions getColumnFamilyOptions(String kvStoreId) {
//        byte[] bytes = rocksDB.get(kvStoreId.getBytes());
//        Properties properties = new Properties();
//        properties.
        return new ColumnFamilyOptions();
    }


    private class Itr implements Iterator<Bucket<ByteString, ByteString>> {

        private RocksIterator rocksIterator;

        private ByteString keyPrefix;

        private boolean scanAll = false;

        public Itr(String storeId, ByteString keyPrefix) {
            rocksIterator = rocksDB.newIterator(getOrCreateColumnFamily(storeId));
            if (!keyPrefix.equals(SCAN_ALL)) {
                rocksIterator.seek(keyPrefix.toByteArray());
            } else {
                rocksIterator.seekToFirst();
                scanAll = true;
            }
            this.keyPrefix = keyPrefix;
        }


        @Override
        public boolean hasNext() {
            boolean valid = false;
            boolean hasKey = false;
            do {
                valid = rocksIterator.isValid();
                if (!valid) {
                    return false;
                }
                if (scanAll) {
                    return true;
                }
                hasKey = ByteString.copyFrom(rocksIterator.key()).startsWith(this.keyPrefix);
                if (!hasKey) {
                    rocksIterator.next();
                    continue;
                }
                return true;
            } while (true);
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

    //
    public static void main(String[] args) {
        StateMachineProperties stateMachineProperties = new StateMachineProperties();
        RocksConfigKeys.setWatch(stateMachineProperties,true);
        RocksStore rocksStore = new RocksStore(stateMachineProperties);

        rocksStore.watch("null",ByteString.copyFromUtf8("1"), opt -> {
            Bucket<ByteString, ByteString> bucket = opt.getBucket();
            OperateType operateType = opt.getOperateType();
            System.out.println("type : " + operateType + " : " + bucket.toString());
        });


        rocksStore.put(null, ByteString.copyFromUtf8("1"), ByteString.copyFromUtf8("1"));
        rocksStore.put(null, ByteString.copyFromUtf8("12"), ByteString.copyFromUtf8("2"));
        rocksStore.put(null, ByteString.copyFromUtf8("13"), ByteString.copyFromUtf8("3"));
        rocksStore.delete(null, ByteString.copyFromUtf8("11"));
        rocksStore.put(null, ByteString.copyFromUtf8("2"), ByteString.copyFromUtf8("2"));
        rocksStore.delete(null, ByteString.copyFromUtf8("2"));
        rocksStore.put(null, ByteString.copyFromUtf8("3"), ByteString.copyFromUtf8("3"));
        rocksStore.put(null, ByteString.copyFromUtf8("4"), ByteString.copyFromUtf8("4"));
        rocksStore.put(null, ByteString.copyFromUtf8("41"), ByteString.copyFromUtf8("4212"));
        rocksStore.put(null, ByteString.copyFromUtf8("42"), ByteString.copyFromUtf8("42113"));

        Set<ByteString> keys = rocksStore.keys(null);
        keys.forEach(key -> {
            System.err.println("key -> " + key);
        });

        Iterator<Bucket<ByteString, ByteString>> scan = rocksStore.scan(null, ByteString.copyFromUtf8("*"));
        scan.forEachRemaining(stringStringBucket -> {
            System.out.println(stringStringBucket.getKey() + " --> " + stringStringBucket.getValue());
        });

    }
}
