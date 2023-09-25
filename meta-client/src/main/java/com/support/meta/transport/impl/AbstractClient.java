package com.support.meta.transport.impl;

import com.support.meta.proto.MetaProtos.*;
import com.support.ratis.client.RatisCoverClient;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.netty.util.internal.StringUtil;

import java.util.Objects;

import static com.support.meta.transport.MetaTransport.metaReplyProtoClientFunction;


public class AbstractClient {

    private static final ByteString SCAN_ALL = ByteString.copyFromUtf8("*");
    private RatisCoverClient<MetaReplyProto> metaReplyProtoClient = null;

    public AbstractClient(RaftClient client) {
        metaReplyProtoClient = metaReplyProtoClientFunction.apply(client);
    }

    public RatisCoverClient<MetaReplyProto> getClient() {
        return metaReplyProtoClient;
    }

    protected MetaRequestProto doRead(MetaRequestProto.RequestCase requestCase, String storeId, ByteString key) {
        MetaRequestProto.Builder metaBuilder = MetaRequestProto.newBuilder();
        switch (requestCase) {
            case LIST:
                ListRequestProto.Builder listBuilder = ListRequestProto.newBuilder();
                if (!StringUtil.isNullOrEmpty(storeId)) {
                    listBuilder.setStore(storeId);
                }
                if (Objects.isNull(key)) {
                    listBuilder.setPrefix(SCAN_ALL);
                } else {
                    listBuilder.setPrefix(key);
                }
                return metaBuilder.setList(listBuilder).build();
            case READ:
                ReadRequestProto.Builder readBuilder = ReadRequestProto.newBuilder();
                if (!StringUtil.isNullOrEmpty(storeId)) {
                    readBuilder.setStore(storeId);
                }
                readBuilder.setKey(key);
                return metaBuilder.setRead(readBuilder).build();
            case LISTKEY:
                ListKeyRequestProto.Builder listKeyBuilder = ListKeyRequestProto.newBuilder();
                if (!StringUtil.isNullOrEmpty(storeId)) {
                    listKeyBuilder.setStore(storeId);
                }
                if (Objects.isNull(key)) {
                    listKeyBuilder.setPrefix(SCAN_ALL);
                } else {
                    listKeyBuilder.setPrefix(key);
                }
                return metaBuilder.setListKey(listKeyBuilder).build();
        }
        throw new RuntimeException("error requestCase : " + requestCase);
    }

    protected MetaRequestProto doWrite(MetaRequestProto.RequestCase requestCase, String storeId, ByteString key, ByteString value) {
        MetaRequestProto.Builder metaBuilder = MetaRequestProto.newBuilder();
        switch (requestCase) {
            case WRITE:
                WriteRequestProto.Builder writeBuilder = WriteRequestProto.newBuilder();
                if (!StringUtil.isNullOrEmpty(storeId)) {
                    writeBuilder.setStore(storeId);
                }
                writeBuilder.addMeta(Meta.newBuilder().setKey(key).setData(value).build());
                return metaBuilder.setWrite(writeBuilder).build();
            case DELETE:
                DeleteRequestProto.Builder deleteBuilder = DeleteRequestProto.newBuilder();
                if (!StringUtil.isNullOrEmpty(storeId)) {
                    deleteBuilder.setStore(storeId);
                }
                deleteBuilder.addKey(key);
                return metaBuilder.setDelete(deleteBuilder).build();
        }
        throw new RuntimeException("error requestCase : " + requestCase);
    }
}
