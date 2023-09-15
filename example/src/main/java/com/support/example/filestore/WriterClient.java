
package com.support.example.filestore;


import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.*;

import static com.support.example.counter.Constants.*;

/**
 * Counter client application, this application sends specific number of
 * INCREMENT command to the Counter cluster and at the end sends a GET command
 * and print the result
 * <p>
 * Parameter to this application indicate the number of INCREMENT command, if no
 * parameter found, application use default value which is 10
 */
public final class WriterClient implements Closeable {

    private int sync = 0;

    private long fileSizeInBytes = 1024;

    private int bufferSizeInBytes = 1024;

    private int numFiles = 10;

    private int numClients = 1;


    private List<File> storageDir = new ArrayList<File>(){{
        add(new File("/tmp/gen/"));
    }};

    private static final int MAX_THREADS_NUM = 1000;

    public int getNumThread() {
        return numFiles < MAX_THREADS_NUM ? numFiles : MAX_THREADS_NUM;
    }

    public long getFileSizeInBytes() {
        return fileSizeInBytes;
    }

    public int getBufferSizeInBytes() {
        return bufferSizeInBytes;
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

        for (File dir : storageDir) {
            FileUtils.createDirectories(dir);
        }

        operation(getClients(raftProperties));
    }

    protected void operation(List<FileStoreClient> clients) throws IOException, ExecutionException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());
        List<String> paths = generateFiles(executor);
//        dropCache();
        System.out.println("Starting Async write now ");

        long startTime = System.currentTimeMillis();

        long totalWrittenBytes = waitWriteFinish(writeByHeapByteBuffer(paths, clients, executor));

        long endTime = System.currentTimeMillis();

        System.out.println("Total files written: " + getNumFiles());
        System.out.println("Each files size: " + getFileSizeInBytes());
        System.out.println("Total data written: " + totalWrittenBytes + " bytes");
        System.out.println("Total time taken: " + (endTime - startTime) + " millis");

        stop(clients);
    }

    private long waitWriteFinish(Map<String, CompletableFuture<List<CompletableFuture<Long>>>> fileMap)
            throws ExecutionException, InterruptedException {
        long totalBytes = 0;
        for (CompletableFuture<List<CompletableFuture<Long>>> futures : fileMap.values()) {
            long writtenLen = 0;
            for (CompletableFuture<Long> future : futures.get()) {
                writtenLen += future.join();
            }

            if (writtenLen != getFileSizeInBytes()) {
                System.out.println("File written:" + writtenLen + " does not match expected:" + getFileSizeInBytes());
            }

            totalBytes += writtenLen;
        }
        return totalBytes;
    }

    private Map<String, CompletableFuture<List<CompletableFuture<Long>>>> writeByHeapByteBuffer(
            List<String> paths, List<FileStoreClient> clients, ExecutorService executor) {
        Map<String, CompletableFuture<List<CompletableFuture<Long>>>> fileMap = new HashMap<>();

        int clientIndex = 0;
        for(String path : paths) {
            final CompletableFuture<List<CompletableFuture<Long>>> future = new CompletableFuture<>();
            final FileStoreClient client = clients.get(clientIndex % clients.size());
            clientIndex ++;
            CompletableFuture.supplyAsync(() -> {
                List<CompletableFuture<Long>> futures = new ArrayList<>();
                File file = new File(path);
                try (FileInputStream fis = new FileInputStream(file)) {
                    final FileChannel in = fis.getChannel();
                    for (long offset = 0L; offset < getFileSizeInBytes(); ) {
                        offset += write(in, offset, client, file.getName(), futures);
                    }
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }

                future.complete(futures);
                return future;
            }, executor);

            fileMap.put(path, future);
        }

        return fileMap;
    }


    protected List<String> generateFiles(ExecutorService executor) {
        UUID uuid = UUID.randomUUID();
        List<String> paths = new ArrayList<>();
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        for (int i = 0; i < numFiles; i ++) {
            String path = getPath("file-" + uuid + "-" + i);
            paths.add(path);
            futures.add(writeFileAsync(path, executor));
        }

        for (int i = 0; i < futures.size(); i ++) {
            long size = futures.get(i).join();
            if (size != fileSizeInBytes) {
                System.err.println("Error: path:" + paths.get(i) + " write:" + size +
                        " mismatch expected size:" + fileSizeInBytes);
            }
        }

        return paths;
    }

    private CompletableFuture<Long> writeFileAsync(String path, ExecutorService executor) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        CompletableFuture.supplyAsync(() -> {
            try {
                future.complete(writeFile(path, fileSizeInBytes, bufferSizeInBytes));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
            return future;
        }, executor);
        return future;
    }

    protected long writeFile(String path, long fileSize, long bufferSize) throws IOException {
        final byte[] buffer = new byte[Math.toIntExact(bufferSize)];
        long offset = 0;
        try(RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
            while (offset < fileSize) {
                final long remaining = fileSize - offset;
                final long chunkSize = Math.min(remaining, bufferSize);
                ThreadLocalRandom.current().nextBytes(buffer);
                raf.write(buffer, 0, Math.toIntExact(chunkSize));
                offset += chunkSize;
            }
        }
        return offset;
    }

    long write(FileChannel in, long offset, FileStoreClient fileStoreClient, String path,
               List<CompletableFuture<Long>> futures) throws IOException {
        final int bufferSize = getBufferSizeInBytes();
        final ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(bufferSize);
        final int bytesRead = buf.writeBytes(in, bufferSize);

        if (bytesRead < 0) {
            throw new IllegalStateException("Failed to read " + bufferSize + " byte(s) from " + this
                    + ". The channel has reached end-of-stream at " + offset);
        } else if (bytesRead > 0) {
            final CompletableFuture<Long> f = fileStoreClient.writeAsync(
                    path, offset, offset + bytesRead == getFileSizeInBytes(), buf.nioBuffer(),
                    sync == 1);
            f.thenRun(buf::release);
            futures.add(f);
        }
        return bytesRead;
    }

    protected void stop(List<FileStoreClient> clients) throws IOException {
        for (FileStoreClient client : clients) {
            client.close();
        }
        System.exit(0);
    }

    public String getPath(String fileName) {
        int hash = fileName.hashCode() % storageDir.size();
        return new File(storageDir.get(Math.abs(hash)), fileName).getAbsolutePath();
    }

    public List<FileStoreClient> getClients(RaftProperties raftProperties) {
        List<FileStoreClient> fileStoreClients = new ArrayList<>();
        for (int i = 0; i < numClients; i ++) {
            final RaftGroup raftGroup = RAFT_GROUP_STORE;

            RaftClient.Builder builder =
                    RaftClient.newBuilder().setProperties(raftProperties);
            builder.setRaftGroup(raftGroup);
            builder.setClientRpc(
                    new GrpcFactory(new org.apache.ratis.conf.Parameters())
                            .newRaftClientRpc(ClientId.randomId(), raftProperties));
            RaftPeer[] peers = RAFT_GROUP_STORE.getPeers().toArray(new RaftPeer[0]);
            builder.setPrimaryDataStreamServer(peers[0]);
            RaftClient client = builder.build();
            fileStoreClients.add(new FileStoreClient(client));
        }
        return fileStoreClients;
    }

    @Override
    public void close() throws IOException {

    }

    public static void main(String[] args) throws Exception {
        WriterClient client = new WriterClient();
        client.run();
    }
}
