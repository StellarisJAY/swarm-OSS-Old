package com.jay.swarm.namenode;

import com.jay.swarm.namenode.network.NameNodeServer;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/8
 **/
public class NameNode {
    public static void main(String[] args) {
        NameNodeServer nameNodeServer = new NameNodeServer();
        nameNodeServer.start();
    }
}
