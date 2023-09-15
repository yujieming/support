package com.support.meta.store;

public class StoreOperate<K, V> {
    private OperateType operateType;
    private Bucket<K, V> bucket;

    public StoreOperate(OperateType operateType, Bucket<K, V> bucket) {
        this.operateType = operateType;
        this.bucket = bucket;
    }

    public OperateType getOperateType() {
        return operateType;
    }

    public Bucket<K, V> getBucket() {
        return bucket;
    }
}
