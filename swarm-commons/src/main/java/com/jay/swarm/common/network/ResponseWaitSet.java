package com.jay.swarm.common.network;

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
public class ResponseWaitSet {
    /**
     * key: id, value: future
     */
    private Map<Integer, CompletableFuture<Object>> waitSet = new ConcurrentHashMap<>(16);

    public void addWaiter(Integer packetId, CompletableFuture<Object> future){
        waitSet.put(packetId, future);
    }

    public void complete(Integer packetId, Object object){
        CompletableFuture<Object> remove = waitSet.remove(packetId);
        if(remove != null){
            remove.complete(object);
        }
    }
}
