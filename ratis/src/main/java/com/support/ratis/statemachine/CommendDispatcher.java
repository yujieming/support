package com.support.ratis.statemachine;

import com.support.ratis.proto.CommandProtos.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public interface CommendDispatcher {

    default CompletableFuture<Reply> dispatch(DispatchStateMachine.DispatchContext context) {
        CompletableFuture<ByteString> result = null;
        try {
            switch (context.getRequest().getRequestCase()) {
                case READ:
                    context.setContent(context.getRequest().getRead().getContent());
                    result = readOnly(context);
                    break;
                case WRITE:
                    context.setContent(context.getRequest().getWrite().getContent());
                    result = write(context);
                    break;
            }
        } catch (Exception e) {
            return JavaUtils.completeExceptionally(e);
        }

        if (Objects.isNull(result)) {
            return JavaUtils.completeExceptionally(new IllegalArgumentException("Invalid Command: " + context.getRequest().getRequestCase()));
        }
        return result.thenApply(content ->
                Reply.newBuilder()
                        .setContent(content).build());
    }

    CompletableFuture<ByteString> readOnly(DispatchStateMachine.DispatchContext context);

    CompletableFuture<ByteString> write(DispatchStateMachine.DispatchContext context);
}
