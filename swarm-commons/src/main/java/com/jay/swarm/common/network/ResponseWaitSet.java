package com.jay.swarm.common.network;

import com.jay.swarm.common.network.entity.NetworkPacket;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *  Response等待队列
 *  客户端发送请求后，这些未完成的请求的CompletableFuture会存在该队列中
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 15:56
 */
@Slf4j
public class ResponseWaitSet {
    /**
     * key: id, value: future
     */
    private final Map<Integer, CompletableFuture<Object>> waitSet = new ConcurrentHashMap<>(256);

    public void addWaiter(Integer packetId, CompletableFuture<Object> future){
        waitSet.put(packetId, future);
    }

    public void complete(Integer packetId, Object object){
        CompletableFuture<Object> remove = waitSet.remove(packetId);
        if(remove != null){
            remove.complete(object);
        }else{
            log.info("no future for id={} type={}", packetId, ((NetworkPacket)object).getType());
        }
    }
}
