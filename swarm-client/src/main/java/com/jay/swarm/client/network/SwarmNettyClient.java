package com.jay.swarm.client.network;

import com.jay.swarm.common.network.entity.NetworkPacket;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.pool.ChannelPool;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/8
 **/
@Slf4j
public class SwarmNettyClient {
    private ChannelProvider channelProvider;
    private UnfinishedRequestContainer unfinishedRequestContainer;

    private static final AtomicInteger ID_PROVIDER = new AtomicInteger(0);


    public SwarmNettyClient(UnfinishedRequestContainer unfinishedRequestContainer) {
        this.channelProvider = new ChannelProvider();
        this.unfinishedRequestContainer = unfinishedRequestContainer;
        this.channelProvider.setUnfinishedRequestContainer(unfinishedRequestContainer);
    }

    public CompletableFuture<NetworkPacket> send(InetSocketAddress address, NetworkPacket packet){
        ChannelPool channelPool = channelProvider.get(address);
        if(channelPool == null){
            throw new RuntimeException("unable to reach NameNode server at " + address.toString());
        }
        // 分配ID
        packet.setId(ID_PROVIDER.getAndIncrement());
        CompletableFuture<NetworkPacket> result = new CompletableFuture<>();
        try{
            Channel channel = channelPool.acquire().get();
            channel.writeAndFlush(packet).addListener((ChannelFutureListener) future -> {
//                if(future.isSuccess()){
//                    if(log.isDebugEnabled()){
//                        log.debug("packet sent to server at {}", address.toString());
//                    }
//                    unfinishedRequestContainer.put(packet.getId(), result);
//                }
//                else{
//                    throw new RuntimeException("unable to send packet to server at " + address.toString());
//                }
                unfinishedRequestContainer.put(packet.getId(), result);
            });

        }catch (InterruptedException | ExecutionException e){
            if(log.isDebugEnabled()){
                log.debug("channel acquire failed", e);
            }
        }
        return result;
    }
}
