package com.support.ratis.test;

import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import static com.support.ratis.test.Constants.RAFT_GROUP;
import static org.apache.ratis.thirdparty.com.google.common.base.Charsets.UTF_8;

public class Server1 {

    public static void main(String[] args) throws IOException {
        final RaftPeer currentPeer = Constants.PEERS.get(0);
        final File storageDir = new File("./" + currentPeer.getId());
        CounterServer counterServer = new CounterServer(currentPeer,storageDir, TimeDuration.ZERO, RAFT_GROUP);
        counterServer.start();
        new Scanner(System.in, UTF_8.name()).nextLine();
    }
}