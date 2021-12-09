package com.jay.swarm.client.network;

import com.jay.swarm.client.network.handler.ClientHandler;
import com.jay.swarm.common.network.handler.PacketDecoder;
import com.jay.swarm.common.network.handler.PacketEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPool;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.SimpleChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/8
 **/
public class ChannelProvider extends AbstractChannelPoolMap<InetSocketAddress, ChannelPool> {
    private final NioEventLoopGroup group = new NioEventLoopGroup();

    private UnfinishedRequestContainer unfinishedRequestContainer;
    @Override
    protected ChannelPool newPool(InetSocketAddress address) {
        Bootstrap bootstrap = new Bootstrap().group(group)
                .channel(NioSocketChannel.class)
                .remoteAddress(address);
        return new SimpleChannelPool(bootstrap, new SimpleChannelPoolHandler());
    }

    class SimpleChannelPoolHandler implements ChannelPoolHandler{

        @Override
        public void channelReleased(Channel channel) throws Exception {
            channel.flush();
        }

        @Override
        public void channelAcquired(Channel channel) throws Exception {

        }

        @Override
        public void channelCreated(Channel channel) throws Exception {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(new PacketDecoder());
            pipeline.addLast(new ClientHandler(unfinishedRequestContainer));
            pipeline.addLast(new PacketEncoder());
        }
    }

    public void setUnfinishedRequestContainer(UnfinishedRequestContainer unfinishedRequestContainer) {
        this.unfinishedRequestContainer = unfinishedRequestContainer;
    }
}
