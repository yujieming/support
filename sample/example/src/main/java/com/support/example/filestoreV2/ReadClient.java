
package com.support.example.filestoreV2;


import com.support.ratis.client.RatisClient;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.proto.ExamplesProtos.*;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static com.support.example.Constants.RAFT_GROUP_STORE_DISPATCH;

/**
 * Counter client application, this application sends specific number of
 * INCREMENT command to the Counter cluster and at the end sends a GET command
 * and print the result
 * <p>
 * Parameter to this application indicate the number of INCREMENT command, if no
 * parameter found, application use default value which is 10
 */
public final class ReadClient implements Closeable {

    private long fileSizeInBytes = 1024;
    private int numFiles = 10;
    private int numClients = 1;

    public long getFileSizeInBytes() {
        return fileSizeInBytes;
    }

    public int getNumFiles() {
        return numFiles;
    }

    public void run() throws Exception {
        int raftSegmentPreallocatedSize = 1024 * 1024 * 1024;
        RaftProperties raftProperties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
        GrpcConfigKeys.setMessageSizeMax(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties,
                SizeInBytes.valueOf(1 * 1024 * 1024 * 1024L));
        RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);

        RaftServerConfigKeys.Log.setSegmentCacheNumMax(raftProperties, 2);

        RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties,
                TimeDuration.valueOf(50000, TimeUnit.MILLISECONDS));
        RaftClientConfigKeys.Async.setOutstandingRequestsMax(raftProperties, 1000);

        operation(getClients(raftProperties));
    }

    protected void operation(List<RatisClient> clients) throws IOException, ExecutionException, InterruptedException {
//        final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());
//        List<String> paths = generateFiles(executor);
//        dropCache();
        System.out.println("Starting Async write now ");

        long startTime = System.currentTimeMillis();

        RatisClient ratisClient = clients.get(0);
        ByteString read = ratisClient.read(read("file-a8897515-d714-4a53-9762-44b2e254d9a7-7", 0, 1024));
//        long totalWrittenBytes = waitWriteFinish(writeByHeapByteBuffer(paths, clients, executor));

        long endTime = System.currentTimeMillis();

        System.out.println("Total files written: " + getNumFiles());
        System.out.println("Each files size: " + getFileSizeInBytes());
        stop(clients);
    }


    private ByteString read(String path, long offset, long length){
        final ReadRequestProto read = ReadRequestProto.newBuilder()
                .setPath(ProtoUtils.toByteString(path))
                .setOffset(offset)
                .setLength(length)
                .build();
        return read.toByteString();
    }

    protected void stop(List<RatisClient> clients) throws IOException {
        for (RatisClient client : clients) {
            client.close();
        }
        System.exit(0);
    }


    public List<RatisClient> getClients(RaftProperties raftProperties) {
        List<RatisClient> fileStoreClients = new ArrayList<>();
        for (int i = 0; i < numClients; i ++) {
            final RaftGroup raftGroup = RAFT_GROUP_STORE_DISPATCH;

            RaftClient.Builder builder =
                    RaftClient.newBuilder().setProperties(raftProperties);
            builder.setRaftGroup(raftGroup);
            builder.setClientRpc(
                    new GrpcFactory(new org.apache.ratis.conf.Parameters())
                            .newRaftClientRpc(ClientId.randomId(), raftProperties));
            RaftPeer[] peers = RAFT_GROUP_STORE_DISPATCH.getPeers().toArray(new RaftPeer[0]);
            builder.setPrimaryDataStreamServer(peers[0]);
            RaftClient client = builder.build();
            fileStoreClients.add(new RatisClient(client));
        }
        return fileStoreClients;
    }

    @Override
    public void close() throws IOException {

    }

    public static void main(String[] args) throws Exception {
        ReadClient client = new ReadClient();
        client.run();
    }
}
