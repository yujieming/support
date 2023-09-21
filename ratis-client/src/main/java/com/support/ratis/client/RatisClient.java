/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.support.ratis.client;

import com.support.ratis.proto.CommandProtos.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A standalone server using raft with a configurable state machine.
 */
public class RatisClient implements Closeable {
    public static final Logger LOG = LoggerFactory.getLogger(RatisClient.class);

    private final RaftClient client;

    public RatisClient(RaftGroup group, RaftProperties properties)
            throws IOException {
        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .build();
    }

    public RatisClient(RaftGroup group, RaftProperties properties, RaftPeer primaryDataStreamServer)
            throws IOException {
        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setPrimaryDataStreamServer(primaryDataStreamServer)
                .build();
    }

    public RatisClient(RaftClient client) {
        this.client = client;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    static ByteString send(
            Message request, CheckedFunction<Message, RaftClientReply, IOException> sendFunction)
            throws IOException {
        final RaftClientReply reply = sendFunction.apply(request);
        final StateMachineException sme = reply.getStateMachineException();
        if (sme != null) {
            throw new IOException("Failed to send request " + request, sme);
        }
        Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
        return reply.getMessage().getContent();
    }

    static ByteString send(
            Message request, RaftPeerId peerId, CheckedBiFunction<Message, RaftPeerId, RaftClientReply, IOException> sendFunction)
            throws IOException {
        final RaftClientReply reply = sendFunction.apply(request, peerId);
        final StateMachineException sme = reply.getStateMachineException();
        if (sme != null) {
            throw new IOException("Failed to send request " + request, sme);
        }
        Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
        return reply.getMessage().getContent();
    }

    static CompletableFuture<ByteString> sendAsync(
            Message request, Function<Message, CompletableFuture<RaftClientReply>> sendFunction) {
        return sendFunction.apply(request).thenApply(reply -> {
            final StateMachineException sme = reply.getStateMachineException();
            if (sme != null) {
                throw new CompletionException("Failed to send request " + request, sme);
            }
            Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
            return reply.getMessage().getContent();
        });
    }

    static CompletableFuture<ByteString> sendAsync(
            Message request, RaftPeerId peerId, BiFunction<Message, RaftPeerId, CompletableFuture<RaftClientReply>> sendFunction) {
        return sendFunction.apply(request, peerId).thenApply(reply -> {
            final StateMachineException sme = reply.getStateMachineException();
            if (sme != null) {
                throw new CompletionException("Failed to send request " + request, sme);
            }
            Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
            return reply.getMessage().getContent();
        });
    }

    private ByteString send(Message request) throws IOException {
        return send(request, client.io()::send);
    }

    private ByteString sendReadOnly(Message request) throws IOException {
        return send(request, client.io()::sendReadOnly);
    }

    private ByteString sendReadOnly(Message request, RaftPeerId peerId) throws IOException {
        return send(request, peerId, client.io()::sendReadOnly);
    }

    private CompletableFuture<ByteString> sendAsync(Message request) {
        return sendAsync(request, client.async()::send);
    }

    private CompletableFuture<ByteString> sendReadOnlyAsync(Message request) {
        return sendAsync(request, client.async()::sendReadOnly);
    }

    private CompletableFuture<ByteString> sendReadOnlyAsync(Message request, RaftPeerId peerId) {
        return sendAsync(request, peerId, client.async()::sendReadOnly);
    }


    public ByteString read(ByteString message) throws IOException {
        final ByteString reply = readImpl(this::sendReadOnly, message);
        return Reply.parseFrom(reply).getContent();
    }

    public CompletableFuture<ByteString> readAsync(ByteString message) {
        return readImpl(this::sendReadOnlyAsync, message
        ).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                () -> Reply.parseFrom(reply).getContent()));
    }

    public ByteString read(ByteString message, RaftPeerId peerId) throws IOException {
        final ByteString reply = readImpl(this::sendReadOnly, message, peerId);
        return Reply.parseFrom(reply).getContent();
    }

    public CompletableFuture<ByteString> readAsync(ByteString message, RaftPeerId peerId) {
        return readImpl(this::sendReadOnlyAsync, message, peerId
        ).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                () -> Reply.parseFrom(reply).getContent()));
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT readImpl(
            CheckedFunction<Message, OUTPUT, THROWABLE> sendReadOnlyFunction,
            ByteString message) throws THROWABLE {

        final CommandRequest request = CommandRequest.newBuilder()
                .setRead(ReadOnlyRequest.newBuilder()
                        .setContent(message)
                ).build();

        return sendReadOnlyFunction.apply(Message.valueOf(request));
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT readImpl(
            CheckedBiFunction<Message, RaftPeerId, OUTPUT, THROWABLE> sendReadOnlyFunction,
            ByteString message, RaftPeerId peerId) throws THROWABLE {

        final CommandRequest request = CommandRequest.newBuilder()
                .setRead(ReadOnlyRequest.newBuilder()
                        .setContent(message)
                ).build();

        return sendReadOnlyFunction.apply(Message.valueOf(request), peerId);
    }


    public ByteString write(ByteString message)
            throws IOException {
        final ByteString reply = writeImpl(this::send, message);
        return Reply.parseFrom(reply).getContent();
    }


    public CompletableFuture<ByteString> writeAsync(ByteString message) {
        return writeImpl(this::sendAsync, message
        ).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                () -> Reply.parseFrom(reply).getContent()));
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT writeImpl(
            CheckedFunction<Message, OUTPUT, THROWABLE> sendFunction,
            ByteString message)
            throws THROWABLE {

        final CommandRequest request = CommandRequest.newBuilder()
                .setWrite(WriteRequest.newBuilder()
                        .setContent(message)
                ).build();
        return sendFunction.apply(Message.valueOf(request));
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT deleteImpl(
            CheckedFunction<Message, OUTPUT, THROWABLE> sendFunction, ByteString message)
            throws THROWABLE {
        return writeImpl(sendFunction, message);
    }

    public ByteString delete(ByteString message) throws IOException {
        final ByteString reply = deleteImpl(this::send, message);
        return Reply.parseFrom(reply).getContent();
    }

    public CompletableFuture<ByteString> deleteAsync(ByteString message) {
        return deleteImpl(this::sendAsync, message
        ).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                () -> Reply.parseFrom(reply).getContent()));
    }
}
