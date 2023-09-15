package com.support.filestore.statemachine;

import com.support.filestore.FileStoreStateMachine;
import com.support.ratis.conf.StateMachineConfigKeys;
import com.support.ratis.conf.StateMachineProperties;
import com.support.ratis.statemachine.StateMachineInit;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.TimeDuration;

public class FileStoreStateMachineInit implements StateMachineInit {

    private RaftProperties properties;

    @Override
    public void config(StateMachineProperties properties) {
        RaftProperties wrapper = StateMachineConfigKeys.wrapper(properties, RaftProperties.class);
        this.properties = wrapper;
    }

    @Override
    public StateMachine init() {
        return new FileStoreStateMachine(properties);
    }
}
