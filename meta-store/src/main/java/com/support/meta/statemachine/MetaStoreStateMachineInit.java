package com.support.meta.statemachine;

import com.support.meta.store.Store;
import com.support.meta.store.rocksdb.RocksConfigKeys;
import com.support.meta.store.rocksdb.RocksStore;
import com.support.ratis.conf.StateMachineProperties;
import com.support.ratis.statemachine.StateMachineInit;
import org.apache.ratis.statemachine.StateMachine;

public class MetaStoreStateMachineInit implements StateMachineInit {

    private Store store;

    @Override
    public void config(StateMachineProperties properties) {
        this.store = new RocksStore(properties);
    }

    @Override
    public StateMachine init() {
        return new MetaStoreStateMachine(store);
    }
}
