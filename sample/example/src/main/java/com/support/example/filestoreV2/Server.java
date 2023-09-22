
package com.support.example.filestoreV2;

import com.support.example.Constants;
import com.support.filestore.FileStoreCommon;
import com.support.ratis.BaseServer;
import com.support.ratis.conf.RaftConfigKeys;
import com.support.ratis.statemachine.WrapperStateMachineRegistry;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.ratis.thirdparty.com.google.common.base.Charsets.UTF_8;

/**
 * Class to start a ratis filestore example server.
 */
public class Server {

    private static int writeThreadNum = 20;

    private static int readThreadNum = 20;

    private static int commitThreadNum = 3;

    private static int deleteThreadNum = 3;

    public static void main(String[] args) throws IOException {

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (RaftPeer peer : Constants.PEERS) {
            final RaftPeer currentPeer = peer;
            final File storageDir = new File("./" + currentPeer.getId());
            RaftProperties properties = getProperties(currentPeer, storageDir);
            BaseServer baseServer = new BaseServer(properties, new WrapperStateMachineRegistry<>(properties));
            executorService.submit(() -> {
                try {
                    baseServer.start();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        new Scanner(System.in, UTF_8.name()).nextLine();
    }

    private static RaftProperties getProperties(RaftPeer currentPeer, File storageDir) {
        RaftProperties properties = new RaftProperties();
        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));
        final int port = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();
        GrpcConfigKeys.Server.setPort(properties, port);

        RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(2, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(3, TimeUnit.SECONDS));

        RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
        RaftServerConfigKeys.Read.setTimeout(properties, TimeDuration.ONE_MINUTE);

        RaftServerConfigKeys.Write.setElementLimit(properties, 40960);
        RaftServerConfigKeys.Write.setByteLimit(properties, SizeInBytes.valueOf("1000MB"));
        ConfUtils.setFiles(properties::setFiles, FileStoreCommon.STATEMACHINE_DIR_KEY, Collections.singletonList(storageDir));
        RaftServerConfigKeys.DataStream.setAsyncRequestThreadPoolSize(properties, writeThreadNum);
        RaftServerConfigKeys.DataStream.setAsyncWriteThreadPoolSize(properties, writeThreadNum);
        ConfUtils.setInt(properties::setInt, FileStoreCommon.STATEMACHINE_WRITE_THREAD_NUM, writeThreadNum);
        ConfUtils.setInt(properties::setInt, FileStoreCommon.STATEMACHINE_READ_THREAD_NUM, readThreadNum);
        ConfUtils.setInt(properties::setInt, FileStoreCommon.STATEMACHINE_COMMIT_THREAD_NUM, commitThreadNum);
        ConfUtils.setInt(properties::setInt, FileStoreCommon.STATEMACHINE_DELETE_THREAD_NUM, deleteThreadNum);

        RaftConfigKeys.setRaftPeerId(properties, currentPeer.getId());
        return properties;
    }
}
