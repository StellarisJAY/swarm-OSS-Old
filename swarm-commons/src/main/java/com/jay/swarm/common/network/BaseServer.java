package com.jay.swarm.common.network;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 15:12
 */
@Slf4j
public class BaseServer {
    private final NioEventLoopGroup boss = new NioEventLoopGroup(1);

    private final NioEventLoopGroup worker = new NioEventLoopGroup();

    private final BaseChannelInitializer channelInitializer = new BaseChannelInitializer();

    public void addHandler(ChannelHandler handler){
        channelInitializer.addHandler(handler);
    }

    public void addHandlers(Collection<ChannelHandler> handlers){
        channelInitializer.addHandlers(handlers);
    }

    public void bind(int port){
        try{
            ServerBootstrap serverBootstrap = new ServerBootstrap().group(boss, worker)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(channelInitializer);

            ChannelFuture channelFuture = serverBootstrap.bind(port).sync();
            log.info("server started");
        } catch (InterruptedException e) {
            log.error("server start failed ", e);
        }
    }
}
