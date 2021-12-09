package com.jay.swarm.client.network;

import com.jay.swarm.common.network.entity.NetworkPacket;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *  未完成请求缓存
 * </p>
 *
 * @author Jay
 * @date 2021/12/8
 **/
@Slf4j
public class UnfinishedRequestContainer {
    private ConcurrentHashMap<Integer, CompletableFuture<NetworkPacket>> container = new ConcurrentHashMap<>(256);

    public void complete(NetworkPacket packet){
        CompletableFuture<NetworkPacket> future = container.remove(packet.getId());
        future.complete(packet);
    }

    public void put(Integer id, CompletableFuture<NetworkPacket> future){
        container.putIfAbsent(id, future);
    }
}
