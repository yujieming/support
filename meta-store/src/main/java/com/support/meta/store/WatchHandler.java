package com.support.meta.store;

import java.util.function.Consumer;


@FunctionalInterface
public interface WatchHandler<K, V> extends Consumer<StoreOperate<K, V>> {

}
