package com.support.ratis.statemachine;

import com.support.ratis.proto.CommandProtos.*;
import org.apache.ratis.proto.ExamplesProtos;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.JavaUtils;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class DispatchStateMachine extends BaseStateMachine {

    private CommendDispatcher dispatcher;


    public DispatchStateMachine() {
    }

    public void setDispatcher(CommendDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public DispatchStateMachine(CommendDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public CommendDispatcher getDispatcher() {
        return dispatcher;
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        final CommandRequest proto;
        try {
            proto = CommandRequest.parseFrom(request.getContent());
        } catch (InvalidProtocolBufferException e) {
            return JavaUtils.completeExceptionally(
                    new IOException("Failed to parse " + request).initCause(e));
        }
        if (proto.getRequestCase() != CommandRequest.RequestCase.READ) {
            return JavaUtils.completeExceptionally(
                    new IllegalArgumentException("Invalid RequestCase: " + proto.getRequestCase()));
        }
        CompletableFuture<Reply> dispatch = dispatch(proto);
        return dispatch
                .thenApply(reply -> Message.valueOf(reply.toByteString()));
    }

    @Override
    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        final ByteString content = request.getMessage().getContent();
        CommandRequest proto = CommandRequest.parseFrom(content);
        final TransactionContext.Builder b = TransactionContext.newBuilder()
                .setStateMachine(this)
                .setClientRequest(request);

        if (proto.getRequestCase() != CommandRequest.RequestCase.WRITE) {
            TransactionContext context = b.build();
            context.setException(new IllegalArgumentException("Invalid RequestCase: " + proto.getRequestCase()));
            return context;
        } else {
            WriteRequest write = proto.getWrite();
            b.setLogData(write.toByteString());
        }
        return b.build();
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();

        final long index = entry.getIndex();
        updateLastAppliedTermIndex(entry.getTerm(), index);

        final RaftProtos.StateMachineLogEntryProto smLog = entry.getStateMachineLogEntry();
        final WriteRequest writeRequest;
        try {
            writeRequest = WriteRequest.parseFrom(smLog.getLogData());
        } catch (InvalidProtocolBufferException e) {
            return JavaUtils.completeExceptionally(
                    new IOException("Failed to parse logData in" + smLog + ", index=" + index).initCause(e));
        }

        CompletableFuture<Reply> dispatch = dispatch(CommandRequest.newBuilder()
                .setWrite(writeRequest).build());
        return dispatch
                .thenApply(reply -> Message.valueOf(reply.toByteString()));
    }

    public CompletableFuture<Reply> dispatch(CommandRequest commandRequest) {
        return getDispatcher().dispatch(commandRequest);
    }
}
