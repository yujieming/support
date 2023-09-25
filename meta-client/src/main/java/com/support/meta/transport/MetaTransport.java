package com.support.meta.transport;

import com.support.meta.proto.MetaProtos.*;
import com.support.ratis.client.RatisCoverClient;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.util.function.Function;

public interface MetaTransport<K, V, R> {

    static Function<RaftClient, RatisCoverClient<MetaReplyProto>> metaReplyProtoClientFunction = client ->
            new RatisCoverClient<MetaReplyProto>(client) {
                @Override
                public MetaReplyProto cover(ByteString content) {
                    try {
                        return MetaReplyProto.parseFrom(content);
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                }
            };

    RaftClient getClient();

    MetaApi<K, V, R> getMetaApi();

    MetaAsyncApi<K, V, R> getMetaAsyncApi();

}
