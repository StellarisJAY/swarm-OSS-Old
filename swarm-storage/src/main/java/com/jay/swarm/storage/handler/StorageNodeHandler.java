package com.jay.swarm.storage.handler;

import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.network.handler.FileTransferHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  StorageNode网络处理器
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
@Slf4j
@ChannelHandler.Sharable
public class StorageNodeHandler extends SimpleChannelInboundHandler<NetworkPacket> {

    /**
     * 文件传输处理器
     */
    private final FileTransferHandler fileTransferHandler;

    public StorageNodeHandler(FileTransferHandler fileTransferHandler) {
        this.fileTransferHandler = fileTransferHandler;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("channel closed by remote address: {}", ctx.channel().remoteAddress());
        ctx.channel().close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if(log.isDebugEnabled()){
            log.debug("channel handler error", cause);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, NetworkPacket packet) {
        short type = packet.getType();
        switch (type){
            // 处理文件传输请求
            case PacketTypes.TRANSFER_FILE_HEAD:
            case PacketTypes.TRANSFER_FILE_BODY:
            case PacketTypes.TRANSFER_FILE_END:
                fileTransferHandler.handle(packet);break;
            default:break;
        }
        NetworkPacket response = NetworkPacket.builder()
                .id(packet.getId())
                .content(null)
                .type(PacketTypes.TRANSFER_RESPONSE).build();
        channelHandlerContext.channel().writeAndFlush(response);
    }
}
