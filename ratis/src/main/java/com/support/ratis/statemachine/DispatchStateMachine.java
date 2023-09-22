package com.support.ratis.statemachine;

import com.support.ratis.proto.CommandProtos.*;
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
        DispatchContext dispatchContext = new DispatchContext();
        dispatchContext.setRequest(proto);
        CompletableFuture<Reply> dispatch = dispatch(dispatchContext);
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
            return b.build().setException(new IllegalArgumentException("Invalid RequestCase: " + proto.getRequestCase()));
        } else {
            WriteRequest write = proto.getWrite();
            b.setLogData(write.getContent());
        }
        doStartTransaction(b, proto.getWrite().getContent());
        return b.build();
    }

    protected void doStartTransaction(TransactionContext.Builder builder, ByteString content) throws IOException {
    }


    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();

        final long index = entry.getIndex();
        updateLastAppliedTermIndex(entry.getTerm(), index);

        final RaftProtos.StateMachineLogEntryProto smLog = entry.getStateMachineLogEntry();
        DispatchContext dispatchContext = new DispatchContext();
        dispatchContext.setTrx(trx);
        dispatchContext.setRequest(CommandRequest.newBuilder()
                .setWrite(WriteRequest.newBuilder().setContent(smLog.getLogData())).build());
        CompletableFuture<Reply> dispatch = dispatch(dispatchContext);
        return dispatch
                .thenApply(reply -> Message.valueOf(reply.toByteString()));
    }

    public CompletableFuture<Reply> dispatch(DispatchContext context) {
        return getDispatcher().dispatch(context);
    }

    public static class DispatchContext {
        private CommandRequest request;
        private ByteString content;
        private TransactionContext trx;

        public CommandRequest getRequest() {
            return request;
        }

        public void setRequest(CommandRequest request) {
            this.request = request;
        }

        public ByteString getContent() {
            return content;
        }

        public void setContent(ByteString content) {
            this.content = content;
        }

        public TransactionContext getTrx() {
            return trx;
        }

        public void setTrx(TransactionContext trx) {
            this.trx = trx;
        }

    }
}
