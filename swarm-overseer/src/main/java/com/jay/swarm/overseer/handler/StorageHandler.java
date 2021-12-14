package com.jay.swarm.overseer.handler;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.StorageInfo;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.overseer.storage.StorageManager;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 19:16
 */
@Slf4j
public class StorageHandler {
    private final StorageManager storageManager;
    private final Serializer serializer;
    public StorageHandler(StorageManager storageManager, Serializer serializer) {
        this.storageManager = storageManager;
        this.serializer = serializer;
    }

    public NetworkPacket handleStorageRegister(NetworkPacket packet, Channel channel){
        try{
            byte[] content = packet.getContent();
            if(content == null){
                throw new RuntimeException("no content found in register packet");
            }
            // 反序列化报文content
            StorageInfo storageInfo = serializer.deserialize(content, StorageInfo.class);
            // 注册存储节点
            boolean status = storageManager.registerStorage(storageInfo, channel);
            log.info("node {} {}", channel.remoteAddress(), status ? "registered" : "failed to register");
            String message = status ? "registered" : "Node ID already taken";
            NetworkPacket response = NetworkPacket.buildPacketOfType(status ? PacketTypes.SUCCESS : PacketTypes.FAIL, message.getBytes(SwarmConstants.DEFAULT_CHARSET));
            response.setId(packet.getId());
            return response;
        }catch (Exception e){
            if(log.isDebugEnabled()){
                log.debug("storage register failed", e);
            }
            NetworkPacket errorResponse = NetworkPacket.buildPacketOfType(PacketTypes.ERROR, e.getMessage().getBytes(SwarmConstants.DEFAULT_CHARSET));
            errorResponse.setId(packet.getId());
            return errorResponse;
        }
    }

    public NetworkPacket handleStorageHeartBeat(NetworkPacket packet){
        try{
            byte[] content = packet.getContent();
            if(content == null){
                throw new RuntimeException("no content found in heart beat packet");
            }
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
