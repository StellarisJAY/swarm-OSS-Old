package com.jay.swarm.overseer.handler;

import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 16:17
 */
@ChannelHandler.Sharable
public class PacketHandler extends SimpleChannelInboundHandler<NetworkPacket> {
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, NetworkPacket packet) throws Exception {
        System.out.println(packet);
        channelHandlerContext.channel().writeAndFlush(NetworkPacket.builder().type(PacketTypes.HEART_BEAT).id(packet.getId()).build());
    }
}
