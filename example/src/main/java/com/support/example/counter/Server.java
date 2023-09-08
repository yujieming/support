package com.support.example.counter;

import com.support.ratis.BaseServer;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.ratis.thirdparty.com.google.common.base.Charsets.UTF_8;

public class Server {

    public static void main(String[] args) throws IOException {

        ExecutorService executorService = Executors.newCachedThreadPool();
        for (RaftPeer peer : Constants.PEERS) {
            final RaftPeer currentPeer = peer;
            final File storageDir = new File("./" + currentPeer.getId());
            RaftProperties properties = getProperties(currentPeer, storageDir);
            BaseServer baseServer = new BaseServer(properties);
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

    private static RaftProperties getProperties(RaftPeer peer, File storageDir) {
        //create a property object
        final RaftProperties properties = new RaftProperties();

        RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

        RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
        RaftServerConfigKeys.Read.setTimeout(properties, TimeDuration.ONE_MINUTE);

        final int port = NetUtils.createSocketAddr(peer.getAddress()).getPort();
        GrpcConfigKeys.Server.setPort(properties, port);

        properties.set("RaftPeerId",peer.getId().toString());

        return properties;
    }
}
