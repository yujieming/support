package com.support.ratis.conf;

import com.support.ratis.statemachine.StateMachineType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public interface StateMachineConfigKeys {
    Logger LOG = LoggerFactory.getLogger(StateMachineConfigKeys.class);

    static Consumer<String> getDefaultLog() {
        return LOG::info;
    }

    String PREFIX = "support.statemachine";

//    String RAFT_GROUP_ID_KEY = PREFIX + ".groupId";

    String RAFT_STATEMACHINE_TYPE = PREFIX + ".type";

    StateMachineType RAFT_STATEMACHINE_TYPE_DEFAULT = null;

    static StateMachineType statemachineType(StateMachineProperties properties) {
        return properties.get(RAFT_STATEMACHINE_TYPE, RAFT_STATEMACHINE_TYPE_DEFAULT, StateMachineType.class);
    }

    static void setStatemachineType(StateMachineProperties properties, StateMachineType stateMachineType) {
        properties.set(RAFT_STATEMACHINE_TYPE, stateMachineType);
    }

}
