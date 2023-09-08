package com.support.ratis.statemachine;

import com.support.extension.ExtensionLoader;
import com.support.ratis.conf.StateMachineConfigKeys;
import com.support.ratis.conf.StateMachineProperties;
import com.support.ratis.converter.RaftGroupIdConverter;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.statemachine.StateMachine;

import java.util.Objects;

public class BaseStateMachineRegistry implements StateMachineRegistry {

    public static final BaseStateMachineRegistry DEFAULT_STATEMACHINE_REGISTRY = new BaseStateMachineRegistry();

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

    /**
     * 修改默认 {@link StateMachineFactory}
     * @param stateMachineFactory
     */
    public void setStateMachineFactory(StateMachineFactory stateMachineFactory) {
        this.stateMachineFactory = stateMachineFactory;
    }

    private StateMachineFactory getStateMachineFactory() {
        return stateMachineFactory;
    }

    /**
     * 初始化 {@link StateMachineProperties}，后续在 {@link StateMachineInit#config(StateMachineProperties)}
     * 和 {@link StateMachineInit#init()} 中根据 stateMachineProperties 创建 {@link StateMachine}
     * @param stateMachineProperties
     */
    protected void preConfig(StateMachineProperties stateMachineProperties) {

    }

    @Override
    public StateMachine apply(RaftGroupId groupId) {
        StateMachineType stateMachineType = RaftGroupIdConverter.convert(groupId);
        StateMachineProperties stateMachineProperties = new StateMachineProperties();
        StateMachineConfigKeys.setStatemachineType(stateMachineProperties, stateMachineType);
        preConfig(stateMachineProperties);
        StateMachine stateMachine = getStateMachineFactory().create(stateMachineProperties);
        return stateMachine;
    }
}
