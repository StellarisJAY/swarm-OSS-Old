package com.jay.swarm.namenode.network;

import com.jay.swarm.common.network.entity.NetworkPacket;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
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
public class PacketHandler extends SimpleChannelInboundHandler<NetworkPacket> {
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, NetworkPacket packet) throws Exception {
        log.info("recv packet: {}", packet.toString());
        channelHandlerContext.channel().writeAndFlush(packet);
    }
}
