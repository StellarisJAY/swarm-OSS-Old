package com.jay.swarm.common.network;

import com.jay.swarm.common.network.handler.PacketDecoder;
import com.jay.swarm.common.network.handler.PacketEncoder;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * <p>
 *  基础ChannelInitializer
 *  提供网络协议编解码器
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 15:15
 */
public class BaseChannelInitializer extends ChannelInitializer<NioSocketChannel> {

    private List<ChannelHandler> handlers = new ArrayList<>();

    @Override
    protected void initChannel(NioSocketChannel nioSocketChannel) throws Exception {
        ChannelPipeline pipeline = nioSocketChannel.pipeline();
        // decoder & encoder
        pipeline.addLast(new PacketDecoder());
        pipeline.addLast(new PacketEncoder());

        // other handlers
        for(ChannelHandler handler : handlers){
            pipeline.addLast(handler);
        }
    }

    public void addHandler(ChannelHandler handler){
        if(handler != null){
            handlers.add(handler);
        }
    }

    public void addHandlers(Collection<ChannelHandler> handlers){
        if(handlers != null && !handlers.isEmpty()){
            this.handlers.addAll(handlers);
        }
    }
}
