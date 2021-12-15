package com.jay.swarm.overseer.handler;

import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.overseer.meta.MetaDataManager;
import com.jay.swarm.overseer.storage.StorageManager;
import com.jay.swarm.overseer.storage.StorageNodeSelector;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;


/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 16:17
 */
@ChannelHandler.Sharable
@Slf4j
public class PacketHandler extends SimpleChannelInboundHandler<NetworkPacket> {

    private final StorageHandler storageHandler;
    private final MetaDataHandler metaDataHandler;

    public PacketHandler(StorageManager storageManager, MetaDataManager metaDataManager, Serializer serializer) {
        this.storageHandler = new StorageHandler(storageManager, serializer);
        StorageNodeSelector storageNodeSelector = new StorageNodeSelector(storageManager);
        this.metaDataHandler = new MetaDataHandler(metaDataManager, storageManager, serializer, storageNodeSelector);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("connection closed by remote address {}", ctx.channel().remoteAddress());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)  {
        if(log.isDebugEnabled()){
            log.debug("exception caught from channel: " + ctx.channel().remoteAddress(), cause);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, NetworkPacket packet) {
        short packetType = packet.getType();
        NetworkPacket response = null;
        // 根据报文类型做不同处理，最终得到一个Response packet
        switch(packetType){
            // 注册存储节点
            case PacketTypes.STORAGE_REGISTER: response = storageHandler.handleStorageRegister(packet, ctx.channel());break;
            // 存储节点心跳
            case PacketTypes.HEART_BEAT: response = storageHandler.handleStorageHeartBeat(packet); break;
            // 上传请求
            case PacketTypes.UPLOAD_REQUEST: response = metaDataHandler.handleUploadRequest(packet); break;
            // 更新meta
            case PacketTypes.UPDATE_FILE_META_STORAGE: response = metaDataHandler.updateFileMeta(packet); break;
            default:break;
        }
        if(response != null){
            // 发送response
            ctx.channel().writeAndFlush(response);
        }
    }


}
