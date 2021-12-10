package com.jay.swarm.common.network.handler;

import com.jay.swarm.common.network.entity.NetworkPacket;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/8
 **/
public class PacketEncoder extends MessageToByteEncoder<NetworkPacket> {
    @Override
    protected void encode(ChannelHandlerContext ctx, NetworkPacket msg, ByteBuf out) throws Exception {
        if(msg != null){
            msg.setLength(msg.getContent() == null ? 0 : msg.getContent().length + NetworkPacket.HEADER_LENGTH);
            NetworkPacket.encode(msg, out);
        }
    }
}
