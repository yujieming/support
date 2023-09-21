package com.support.meta.statemachine;

import com.support.meta.proto.MetaProtos.*;
import com.support.meta.store.Bucket;
import com.support.meta.store.Store;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public abstract class MetaCommandDispatcher implements Dispatcher<MetaRequestProto, MetaReplyProto> {

    abstract Store<ByteString, ByteString> getStore();

    @Override
    public CompletableFuture<MetaReplyProto> dispatch(MetaRequestProto requestProto) {
        switch (requestProto.getRequestCase()) {
            case LIST:
                ListRequestProto list = requestProto.getList();
                ByteString prefix = list.getPrefix();
                Iterator<Bucket<ByteString, ByteString>> scan = getStore().scan(prefix);
                ListReplyProto.Builder listBuilder = ListReplyProto.newBuilder().
                        setReply(ReplyProto.newBuilder().setSuccess(true));
                scan.forEachRemaining(bucket -> {
                    listBuilder.addMetas(Meta.newBuilder().setKey(bucket.getKey()).setData(bucket.getValue()).build());
                });
                return CompletableFuture.completedFuture(MetaReplyProto.newBuilder().setList(listBuilder).build());
            case READ:
                Supplier<MetaReplyProto> supplier = () -> {
                    ReadRequestProto read = requestProto.getRead();
                    ByteString key = read.getKey();
                    ByteString value = getStore().get(key);
                    ReadReplyProto.Builder builder = ReadReplyProto.newBuilder().setReply(ReplyProto.newBuilder().setSuccess(true))
                            .setMeta(Meta.newBuilder().setData(value).setKey(key));
                    return MetaReplyProto.newBuilder().setRead(builder).build();
                };
                return CompletableFuture.completedFuture(supplier.get());

            case WRITE:
                WriteRequestProto write = requestProto.getWrite();
                List<Meta> metaList = write.getMetaList();
                metaList.forEach(meta -> {
                    getStore().put(meta.getKey(), meta.getData());
                });
                return CompletableFuture.completedFuture(MetaReplyProto.newBuilder()
                        .setReply(ReplyProto.newBuilder().setSuccess(true)).build());
            case DELETE:
                DeleteRequestProto delete = requestProto.getDelete();
                List<ByteString> keyList = delete.getKeyList();
                keyList.forEach(key -> {
                    getStore().delete(key);
                });
                return CompletableFuture.completedFuture(MetaReplyProto.newBuilder()
                        .setReply(ReplyProto.newBuilder().setSuccess(true)).build());
        }
        return JavaUtils.completeExceptionally(new IllegalArgumentException("Invalid Command: " + requestProto.getRequestCase()));
    }
}
