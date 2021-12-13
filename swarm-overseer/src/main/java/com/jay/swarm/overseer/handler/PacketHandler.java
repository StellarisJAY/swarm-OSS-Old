package com.jay.swarm.overseer.handler;

import com.jay.swarm.common.entity.StorageInfo;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.overseer.meta.MetaDataManager;
import com.jay.swarm.overseer.storage.StorageManager;
import io.netty.channel.Channel;
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

    private final StorageManager storageManager;
    private final MetaDataManager metaDataManager;

    public PacketHandler(StorageManager storageManager, MetaDataManager metaDataManager) {
        this.storageManager = storageManager;
        this.metaDataManager = metaDataManager;
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        log.info("connection closed by remote address {}", ctx.channel().remoteAddress());
        storageManager.removeChannel(ctx.channel());
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
            case PacketTypes.STORAGE_REGISTER: response = handleStorageRegister(packet, ctx.channel());break;
            // 存储节点心跳
            case PacketTypes.HEART_BEAT: response = handleStorageHeartBeat(packet); break;

            default:break;
        }

        if(response != null){
            // 发送response
            ctx.channel().writeAndFlush(response);
        }
    }

    private NetworkPacket handleStorageRegister(NetworkPacket packet, Channel channel){
        try{
            byte[] content = packet.getContent();
            if(content == null){
                throw new RuntimeException("no content found in register packet");
            }
            Serializer serializer = new ProtoStuffSerializer();
            // 反序列化报文content
            StorageInfo storageInfo = serializer.deserialize(content, StorageInfo.class);
            // 注册存储节点
            boolean status = storageManager.registerStorage(storageInfo, channel);
            log.info("Storage Node {} registered", storageInfo.getId());
            return NetworkPacket.builder()
                    .type(PacketTypes.STORAGE_REGISTER_RESPONSE)
                    .id(packet.getId())
                    .content(status ? "success".getBytes() : "fail".getBytes())
                    .build();
        }catch (Exception e){
            if(log.isDebugEnabled()){
                log.debug("storage register failed", e);
            }
            return NetworkPacket.builder()
                    .type(PacketTypes.STORAGE_REGISTER_RESPONSE)
                    .id(packet.getId())
                    .content(e.getMessage().getBytes())
                    .build();
        }
    }

    private NetworkPacket handleStorageHeartBeat(NetworkPacket packet){
        try{
            byte[] content = packet.getContent();
            if(content == null){
                throw new RuntimeException("no content found in heart beat packet");
            }
            Serializer serializer = new ProtoStuffSerializer();
            StorageInfo storageInfo = serializer.deserialize(content, StorageInfo.class);
            storageManager.storageHeartBeat(storageInfo);

            return NetworkPacket.builder()
                    .type(PacketTypes.HEART_BEAT)
                    .id(packet.getId())
                    .content("done".getBytes())
                    .build();
        } catch (Exception e) {
            if (log.isDebugEnabled()){
                log.debug("heart beat failed", e);
            }
            return NetworkPacket.builder()
                    .type(PacketTypes.HEART_BEAT)
                    .id(packet.getId())
                    .content("fail".getBytes())
                    .build();
        }
    }
}
