package com.jay.swarm.common.network;

import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.handler.BaseClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 15:27
 */
@Slf4j
public class BaseClient {

    private final BaseChannelInitializer channelInitializer = new BaseChannelInitializer();
    private final EventLoopGroup group = new NioEventLoopGroup();
    private Channel channel;
    private ResponseWaitSet responseWaitSet = new ResponseWaitSet();

    private AtomicInteger idProvider = new AtomicInteger(1);


    public BaseClient() {
    }

    public void addHandler(ChannelHandler handler){
        channelInitializer.addHandler(handler);
    }

    public void addHandlers(Collection<ChannelHandler> handlers){
        channelInitializer.addHandlers(handlers);
    }

    public void connect(String host, int port){
        try{
            Bootstrap bootstrap = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(channelInitializer)
                    .remoteAddress(new InetSocketAddress(host, port));
            addHandler(new BaseClientHandler(responseWaitSet));
            ChannelFuture channelFuture = bootstrap.connect().sync();
            channel = channelFuture.channel();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public CompletableFuture<Object> sendAsync(NetworkPacket packet){
        CompletableFuture<Object> result = new CompletableFuture<>();
        packet.setId(idProvider.getAndIncrement());
        channel.writeAndFlush(packet).addListener((ChannelFutureListener)listener->{
            responseWaitSet.addWaiter(packet.getId(), result);
        });

        return result;
    }
}
