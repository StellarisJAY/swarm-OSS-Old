package com.jay.swarm.common.network;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.network.callback.FileTransferCallback;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.util.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * <p>
 *  文件分片发送器
 *  将文件分片后发送
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 15:17
 */
@Slf4j
public class ShardedFileSender {
    private final Channel channel;

    private final BaseClient client;

    private final FileTransferCallback callback;

    public ShardedFileSender(Channel channel, FileTransferCallback callback) {
        this.channel = channel;
        this.callback = callback;
        this.client = null;
    }

    public ShardedFileSender(BaseClient client, FileTransferCallback callback) {
        this.client = client;
        this.callback = callback;
        this.channel = null;
    }

    public void send(File file,  String fileId){
        send(null, 0, file, fileId);
    }

    public void send(String host, int port, File file, String fileId){
        long size = file.length();
        try(FileInputStream inputStream = new FileInputStream(file); FileChannel fileChannel = inputStream.getChannel()){
            int shardIndex = 0;
            while(size > 0){
                ByteBuffer data = ByteBuffer.allocate(SwarmConstants.DEFAULT_SHARD_SIZE + fileId.length());
                data.put(fileId.getBytes(SwarmConstants.DEFAULT_CHARSET));
                int length = fileChannel.read(data);
                data.flip();
                ByteBuf dataBuf = Unpooled.wrappedBuffer(data);
                if(client != null && !StringUtils.isEmpty(host)){
                    client.sendAsync(host, port, PacketTypes.TRANSFER_FILE_BODY, dataBuf);
                }else if(channel != null){
                    ByteBuf packet = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_BODY, dataBuf);
                    channel.writeAndFlush(packet);
                }
                size -= length;
            }
        }catch (IOException e){
            log.error("file shard sending error: ", e);
        }
    }
}
