package com.jay.swarm.client.network.handler;

import com.jay.swarm.client.network.UnfinishedRequestContainer;
import com.jay.swarm.common.network.entity.NetworkPacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/8
 **/
public class ClientHandler extends SimpleChannelInboundHandler<NetworkPacket> {

    private UnfinishedRequestContainer unfinishedRequestContainer;

    public ClientHandler(UnfinishedRequestContainer unfinishedRequestContainer) {
        this.unfinishedRequestContainer = unfinishedRequestContainer;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, NetworkPacket packet) throws Exception {
        int id = packet.getId();
        unfinishedRequestContainer.complete(packet);
    }
}
