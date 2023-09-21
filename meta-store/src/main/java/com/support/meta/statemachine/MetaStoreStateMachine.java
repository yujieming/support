package com.support.meta.statemachine;

import com.support.meta.proto.MetaProtos.*;
import com.support.meta.store.Bucket;
import com.support.meta.store.Store;
import org.apache.ratis.proto.ExamplesProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.JavaUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;

public class MetaStoreStateMachine extends BaseStateMachine {

    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

//    private Store<ByteString, ByteString> store;

    private MetaCommandDispatcher dispatcher;

    public MetaStoreStateMachine(Store<ByteString, ByteString> store) {
        dispatcher = new MetaCommandDispatcher() {
            @Override
            Store<ByteString, ByteString> getStore() {
                return store;
            }
        };
    }

    @Override
    public void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage) throws IOException {
        super.initialize(raftServer, raftGroupId, storage);
    }

    @Override
    public void reinitialize() throws IOException {
        super.reinitialize();
    }

    @Override
    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        final ByteString content = request.getMessage().getContent();
        final MetaRequestProto proto = MetaRequestProto.parseFrom(content);
        final TransactionContext.Builder b = TransactionContext.newBuilder()
                .setStateMachine(this)
                .setClientRequest(request);

        return super.startTransaction(request);
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        return super.applyTransaction(trx);
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        final ByteString content = request.getContent();
        MetaRequestProto requestProto = null;
        try {
            requestProto = MetaRequestProto.parseFrom(content);
            switch (requestProto.getRequestCase()) {
                case LIST:
                case READ:
                default:
                    JavaUtils.completeExceptionally(new IllegalArgumentException("Invalid Command: " + requestProto.getRequestCase()));
            }
        } catch (InvalidProtocolBufferException e) {
            return JavaUtils.completeExceptionally(
                    new IOException("Failed to parse " + request).initCause(e));
        }
        return dispatcher.dispatch(requestProto)
                .thenApply(reply -> Message.valueOf(reply.toByteString()));
    }
}
