package com.jay.swarm.overseer.handler;

import com.jay.swarm.common.entity.Storage;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.overseer.meta.MetaDataManager;
import com.jay.swarm.overseer.storage.StorageManager;
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
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, NetworkPacket packet) {
        short packetType = packet.getType();
        NetworkPacket response = null;
        // 根据报文类型做不同处理，最终得到一个Response packet
        switch(packetType){
            // 注册存储节点
            case PacketTypes.STORAGE_REGISTER: response = handleStorageRegister(packet);break;
            // 存储节点心跳
            case PacketTypes.HEART_BEAT: response = handleStorageHeartBeat(packet); break;

            default:break;
        }

        // 发送response
        channelHandlerContext.channel().writeAndFlush(response);
    }

    private NetworkPacket handleStorageRegister(NetworkPacket packet){
        try{
            byte[] content = packet.getContent();
            if(content == null){
                throw new RuntimeException("no content found in register packet");
            }
            Serializer serializer = new ProtoStuffSerializer();
            // 反序列化报文content
            Storage storage = serializer.deserialize(content, Storage.class);
            // 注册存储节点
            boolean status = storageManager.registerStorage(storage);
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
            Storage storage = serializer.deserialize(content, Storage.class);
            storageManager.storageHeartBeat(storage);

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
