package com.support.meta.message;

import com.support.meta.MetaTransportClient;
import com.support.meta.proto.MetaProtos.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.support.meta.message.NotExistMessage.NOT_EXIST;


public class MessageTransport<M extends Message> {

    private MetaTransportClient metaTransport;

    private M TEMPLE;

    private MessageTransport(MetaTransportClient metaTransport, M message) {
        this.metaTransport = metaTransport;
        this.TEMPLE = message;
    }

    public static class Builder<M extends Message> {
        private MetaTransportClient metaTransport;
        private M message;

        public Builder<M> setMetaTransportClient(MetaTransportClient transportClient) {
            this.metaTransport = transportClient;
            return this;
        }

        public Builder<M> setMetaTransportClient(M message) {
            this.message = message;
            return this;
        }

        public MessageTransport<M> builder() {
            return new MessageTransport<M>(metaTransport, message);
        }

    }

    public CompletableFuture<Set<String>> keys() {
        return metaTransport.getMetaAsyncApi()
                .metaKeys(TEMPLE.getType(), null)
                .thenApply(reply ->
                        reply.getListKey()
                                .getKeyList()
                                .asByteStringList()
                                .stream()
                                .map(ByteString::toStringUtf8)
                                .collect(Collectors.toSet())
                );
    }

    public CompletableFuture<Map<String, ? extends Message>> list() {
        return metaTransport.getMetaAsyncApi()
                .metaList(TEMPLE.getType(), null)
                .thenApply(reply ->
                        reply.getList()
                                .getMetasList()

                ).thenApply(metas -> {
                    HashMap map = new HashMap<>();
                    for (Meta meta : metas) {
                        map.put(meta.getKey().toStringUtf8(), Message.toMessage(meta.getData(), TEMPLE.getClass()));
                    }
                    return map;
                });
    }

    public CompletableFuture<? extends Message> get(String key) {
        return metaTransport.getMetaAsyncApi()
                .get(TEMPLE.getType(), ByteString.copyFromUtf8(key))
                .thenApply(reply ->
                        reply.getRead()
                                .getMeta()
                                .getData()
                ).thenApply(bytes -> {
                    if (Objects.isNull(bytes)) {
                        return NOT_EXIST;
                    } else {
                        return Message.toMessage(bytes, TEMPLE.getClass());
                    }
                });
    }

    public CompletableFuture<Boolean> delete(String key) {

        return metaTransport.getMetaAsyncApi()
                .delete(TEMPLE.getType(), ByteString.copyFromUtf8(key))
                .thenApply(reply ->
                        reply.getBase()
                                .getSuccess()
                );
    }


    public CompletableFuture<Boolean> put(String key, M message) {
        return metaTransport.getMetaAsyncApi()
                .put(TEMPLE.getType(), ByteString.copyFromUtf8(key), Message.writeObject2ByteString(message))
                .thenApply(reply ->
                        reply.getBase()
                                .getSuccess()
                );
    }
}


