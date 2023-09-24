package com.support.meta.store.rocksdb;

import com.support.meta.store.Store;
import com.support.ratis.conf.RaftConfigKeys;
import com.support.ratis.conf.StateMachineProperties;
import org.rocksdb.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

import static org.apache.ratis.conf.ConfUtils.set;

public interface RocksConfigKeys {
    Logger LOG = LoggerFactory.getLogger(RaftConfigKeys.class);

    static Consumer<String> getDefaultLog() {
        return LOG::info;
    }

    String PREFIX = "support.rocks";

    String ROCKS_PATH = PREFIX + ".path";

    String ROCKS_PATH_DEFAULT = "/tmp/rocks-db/";

    static String rocksPath(StateMachineProperties properties) {
        return properties.get(ROCKS_PATH, ROCKS_PATH_DEFAULT, String.class);
    }

    static void setRocksPath(StateMachineProperties properties, String path) {
        set(properties::set, ROCKS_PATH, path);
    }


    String ROCKS_DIR = PREFIX + ".dir";

    String ROCKS_DIR_DEFAULT = "meta.db";

    static String rocksDir(StateMachineProperties properties) {
        return properties.get(ROCKS_DIR, ROCKS_DIR_DEFAULT, String.class);
    }

    static void setRocksDir(StateMachineProperties properties, String dir) {
        set(properties::set, ROCKS_DIR, dir);
    }


    String ROCKS_OPTIONS = PREFIX + ".options";

    Options ROCKS_OPTIONS_DEFAULT = new Options()
            .setCreateIfMissing(true);

    static Options rocksOptions(StateMachineProperties properties) {
        return properties.get(ROCKS_OPTIONS, ROCKS_OPTIONS_DEFAULT, Options.class);
    }

    static void setRocksOptions(StateMachineProperties properties, Options options) {
        set(properties::set, ROCKS_OPTIONS, options);
    }

    String STORE = PREFIX + ".store";

    static Store store(StateMachineProperties properties) {
        return properties.get(STORE, null, Store.class);
    }

    static void setStore(StateMachineProperties properties, Store store) {
        set(properties::set, STORE, store);
    }

    String WATCH = PREFIX + ".watch";
    Boolean ROCKS_WATCH_DEFAULT = false;

    static Boolean watch(StateMachineProperties properties) {
        return properties.get(WATCH, ROCKS_WATCH_DEFAULT, Boolean.class);
    }

    static void setWatch(StateMachineProperties properties, Boolean watch) {
        set(properties::set, WATCH, watch);
    }
}
