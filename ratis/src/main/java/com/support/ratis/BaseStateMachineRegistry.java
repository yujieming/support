package com.support.ratis;

import com.support.extension.ExtensionLoader;
import com.support.ratis.conf.StateMachineConfigKeys;
import com.support.ratis.conf.StateMachineProperties;
import com.support.ratis.converter.RaftGroupIdConverter;
import com.support.ratis.statemachine.StateMachineFactory;
import com.support.ratis.statemachine.StateMachineInit;
import com.support.ratis.statemachine.StateMachineType;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.statemachine.StateMachine;

import java.util.Objects;

public class BaseStateMachineRegistry implements StateMachineRegistry {

    private StateMachineFactory stateMachineFactory = (stateMachineProperties) -> {

        StateMachineType stateMachineType = StateMachineConfigKeys.statemachineType(stateMachineProperties);
        if (Objects.isNull(stateMachineType)) {
            throw new RuntimeException("not support");
        }
        ExtensionLoader<StateMachineInit> stateMachineInitExtensionLoader =
                ExtensionLoader.getExtensionLoader(StateMachineInit.class);
        if (stateMachineInitExtensionLoader.hasExtension(stateMachineType.getType())) {
            StateMachineInit stateMachineInit = stateMachineInitExtensionLoader.getLoadedExtension(stateMachineType.getType());
            return stateMachineInit;
        }
        throw new RuntimeException("not exist stateMachineInit");
    };


    @Override
    public StateMachine apply(RaftGroupId groupId) {
        StateMachineType stateMachineType = RaftGroupIdConverter.convert(groupId);
        StateMachineProperties stateMachineProperties = new StateMachineProperties();
        StateMachineConfigKeys.setStatemachineType(stateMachineProperties, stateMachineType);
        StateMachine stateMachine = stateMachineFactory.create(stateMachineProperties);
        return stateMachine;
    }
}
