package com.jay.swarm.namenode.network;

import com.jay.swarm.common.network.handler.PacketDecoder;
import com.jay.swarm.common.network.handler.PacketEncoder;
import com.jay.swarm.common.util.PropertyUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/8
 **/
@Slf4j
public class NameNodeServer {
    private NioEventLoopGroup boss = new NioEventLoopGroup(1);
    private NioEventLoopGroup worker = new NioEventLoopGroup();

    private ServerBootstrap bootstrap;

    public NameNodeServer() {
        bootstrap = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        ChannelPipeline pipeline = socketChannel.pipeline();
                        pipeline.addLast(new PacketDecoder());
                        pipeline.addLast(new PacketHandler());
                        pipeline.addLast(new PacketEncoder());
                    }
                });
    }

    public void start(){
        try{
            int port = Integer.parseInt(PropertyUtil.get("server.port"));

            ChannelFuture future = bootstrap.bind(port).sync();
            if(future.isSuccess()){
                log.info("swarm namenode started");
            }
        }catch (NumberFormatException e){
            log.error("invalid port config in namenode.properties", e);
        }catch (InterruptedException e){
            log.error("unable to start namenode server due to: ", e);
        }
    }
}
