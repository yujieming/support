package com.support.meta.statemachine;

import com.support.meta.proto.MetaProtos.*;
import com.support.meta.store.Bucket;
import com.support.meta.store.Store;
import com.support.ratis.statemachine.CommendDispatcher;
import com.support.ratis.statemachine.DispatchStateMachine;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.AbstractMessageLite;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MetaStoreStateMachine extends DispatchStateMachine {

    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

    private final Store<ByteString, ByteString> store;

    public MetaStoreStateMachine(Store<ByteString, ByteString> store) {
        super();
        setDispatcher(new Dispatcher());
        this.store = store;
    }

    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage raftStorage) throws IOException {
        super.initialize(raftServer, raftGroupId, raftStorage);
        this.storage.init(raftStorage);
    }

    @Override
    public void reinitialize() throws IOException {
        super.reinitialize();
    }

    @Override
    public StateMachineStorage getStateMachineStorage() {
        return storage;
    }

    @Override
    public void close() throws IOException {
        super.close();
        store.close();
    }

    private class Dispatcher implements CommendDispatcher {
        @Override
        public CompletableFuture<ByteString> readOnly(DispatchStateMachine.DispatchContext context) {
            MetaRequestProto requestProto;
            try {
                requestProto = MetaRequestProto.parseFrom(context.getContent());
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e.getMessage());
            }
            switch (requestProto.getRequestCase()) {
                case LISTKEY:
                    ListKeyRequestProto listKey = requestProto.getListKey();
                    Set<ByteString> keys = store.keys(listKey.getStore());
                    Set<String> collect = null;
                    if (Objects.nonNull(keys)) {
                        collect = keys.stream().map(ByteString::toStringUtf8).collect(Collectors.toSet());
                    }

                    return CompletableFuture.completedFuture(MetaReplyProto.newBuilder()
                                    .setListKey(ListKeyReplyProto.newBuilder()
                                            .addAllKey(collect)
                                            .build()).build())
                            .thenApply(AbstractMessageLite::toByteString);
                case LIST:
                    ListRequestProto list = requestProto.getList();
                    ByteString prefix = list.getPrefix();
                    Iterator<Bucket<ByteString, ByteString>> scan = store.scan(list.getStore(), prefix);
                    ListReplyProto.Builder listBuilder = ListReplyProto.newBuilder().
                            setReply(ReplyProto.newBuilder().setSuccess(true));
                    scan.forEachRemaining(bucket -> {
                        listBuilder.addMetas(Meta.newBuilder().setKey(bucket.getKey()).setData(bucket.getValue()).build());
                    });
                    return CompletableFuture.completedFuture(MetaReplyProto.newBuilder().setList(listBuilder).build().toByteString());
                case READ:
                    Supplier<MetaReplyProto> supplier = () -> {
                        ReadRequestProto read = requestProto.getRead();
                        ByteString key = read.getKey();
                        ByteString value = store.get(read.getStore(), key);
                        Meta.Builder meta = Meta.newBuilder().setKey(key);
                        if (Objects.nonNull(value)) {
                            meta.setData(value);
                        }
                        ReadReplyProto.Builder builder = ReadReplyProto.newBuilder()
                                .setReply(ReplyProto.newBuilder().setSuccess(true))
                                .setMeta(meta);
                        return MetaReplyProto.newBuilder().setRead(builder).build();
                    };
                    return CompletableFuture.completedFuture(supplier.get())
                            .thenApply(AbstractMessageLite::toByteString);
            }
            throw new IllegalArgumentException("Invalid Command: " + requestProto.getRequestCase());
        }

        @Override
        public CompletableFuture<ByteString> write(DispatchStateMachine.DispatchContext context) {
            MetaRequestProto requestProto;
            try {
                requestProto = MetaRequestProto.parseFrom(context.getContent());
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e.getMessage());
            }
            switch (requestProto.getRequestCase()) {
                case WRITE:
                    WriteRequestProto write = requestProto.getWrite();
                    List<Meta> metaList = write.getMetaList();
                    metaList.forEach(meta -> {
                        store.put(write.getStore(), meta.getKey(), meta.getData());
                    });
                    return CompletableFuture.completedFuture(MetaReplyProto.newBuilder()
                            .setBase(ReplyProto.newBuilder().setSuccess(true)).build().toByteString());
                case DELETE:
                    DeleteRequestProto delete = requestProto.getDelete();
                    List<ByteString> keyList = delete.getKeyList();
                    keyList.forEach(key -> {
                        store.delete(delete.getStore(), key);
                    });
                    return CompletableFuture.completedFuture(MetaReplyProto.newBuilder()
                            .setBase(ReplyProto.newBuilder().setSuccess(true)).build().toByteString());
            }
            throw new IllegalArgumentException("Invalid Command: " + requestProto.getRequestCase());
        }
    }
}
