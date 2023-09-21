package com.support.ratis.statemachine;

import com.support.ratis.proto.CommandProtos.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public interface CommendDispatcher {

    default CompletableFuture<Reply> dispatch(CommandRequest request) {
        CompletableFuture<ByteString> result = null;
        switch (request.getRequestCase()) {
            case READ:
                result = readOnly(request.getRead().getContent());
                break;
            case WRITE:
                result = write(request.getWrite().getContent());
                break;
        }
        if (Objects.isNull(result)) {
            return JavaUtils.completeExceptionally(new IllegalArgumentException("Invalid Command: " + request.getRequestCase()));
        }
        return result.thenApply(content ->
                Reply.newBuilder()
                        .setContent(content).build());
    }

    CompletableFuture<ByteString> readOnly(ByteString content);

    CompletableFuture<ByteString> write(ByteString content);
}
