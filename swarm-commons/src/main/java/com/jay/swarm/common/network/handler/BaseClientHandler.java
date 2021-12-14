package com.jay.swarm.common.network.handler;

import com.jay.swarm.common.network.ResponseWaitSet;
import com.jay.swarm.common.network.entity.NetworkPacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 16:19
 */
public class BaseClientHandler extends SimpleChannelInboundHandler<NetworkPacket> {
    private final ResponseWaitSet responseWaitSet;

    public BaseClientHandler(ResponseWaitSet responseWaitSet) {
        this.responseWaitSet = responseWaitSet;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, NetworkPacket packet) {
        if(packet != null){
            int id = packet.getId();
            responseWaitSet.complete(id, packet);
        }
    }
}
