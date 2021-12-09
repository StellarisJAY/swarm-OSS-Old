package com.jay.swarm.common.network.handler;

import com.jay.swarm.common.network.entity.NetworkPacket;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  网络通信报文解析器
 *  通过LengthFieldBasedFrameDecoder解决TCP粘包
 * </p>
 *
 * @author Jay
 * @date 2021/12/8
 **/
@Slf4j
public class PacketDecoder extends LengthFieldBasedFrameDecoder {

    /**
     * LengthFieldBasedFrameDecoder 解决TCP粘包
     */
    public PacketDecoder() {
        /*
            lengthFieldOffset = 2： 长度字段在报文中的偏移是2.
            lengthFieldLength = 4： 长度字段int类型，共4字节。
            lengthAdjustment = -6: 需要整个报文，报文结尾 = length结尾下标 + length - adjustment
         */
        super(NetworkPacket.MAX_PACKET_LENGTH, 2, 4, -6, 0);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        try{
            // 父类解码
            Object object = super.decode(ctx, in);
            if(object instanceof ByteBuf){
                // 解码出NetworkPacket
                return NetworkPacket.decode((ByteBuf) object);
            }
        }catch (RuntimeException e){
            log.error("packet decode error: ", e);
        }
        return null;
    }
}
