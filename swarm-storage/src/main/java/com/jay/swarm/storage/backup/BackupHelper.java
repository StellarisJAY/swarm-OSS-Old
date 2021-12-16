package com.jay.swarm.storage.backup;

import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.FileUploadEnd;
import com.jay.swarm.common.entity.StorageInfo;
import com.jay.swarm.common.fs.FileInfo;
import com.jay.swarm.common.network.BaseClient;
import com.jay.swarm.common.network.ShardedFileSender;
import com.jay.swarm.common.network.callback.DefaultFileTransferCallback;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.Serializer;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/15 19:22
 */
@Slf4j
public class BackupHelper {
    private volatile BaseClient client;
    private final Serializer serializer;

    public BackupHelper(Serializer serializer) {
        this.serializer = serializer;
    }

    public void sendBackup(FileInfo fileInfo, String path, List<StorageInfo> storages){
        if(storages != null && !storages.isEmpty()){
            StorageInfo targetNode = storages.remove(0);
            int retryTimes = 0;
            // 尝试同步备份
            while(retryTimes < SwarmConstants.BACKUP_SYNC_RETRY_TIMES){
                try{
                    long start = System.currentTimeMillis();
                    // 目标节点地址
                    String host = targetNode.getHost();
                    int port = targetNode.getPort();
                    // 封装HEAD报文
                    NetworkPacket head = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_HEAD, serializer.serialize(fileInfo, FileInfo.class));

                    // 建立连接
                    BaseClient client = getClient();
                    client.connect(host, port);
                    // 发送HEAD，等待回复
                    NetworkPacket headResponse = (NetworkPacket) client.sendAsync(head).get();

                    // 发送文件数据
                    ShardedFileSender shardedFileSender = new ShardedFileSender(client, serializer, new DefaultFileTransferCallback());
                    shardedFileSender.send(new File(path), fileInfo.getFileId());

                    // 封装END报文
                    FileUploadEnd uploadEnd = FileUploadEnd.builder().fileId(fileInfo.getFileId()).otherStorages(storages).build();
                    NetworkPacket end = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_END, serializer.serialize(uploadEnd, FileUploadEnd.class));
                    // 发送END，等待最终返回
                    NetworkPacket finalResponse = (NetworkPacket) client.sendAsync(end).get();

                    if(finalResponse.getType() == PacketTypes.SUCCESS){
                        log.info("backup synchronized to peer storage node, time used {} ms", (System.currentTimeMillis() - start));
                        // 备份成功，结束循环
                        break;
                    }
                    else{
                        // 失败，重试
                        retryTimes++;
                    }
                }catch (Exception e){
                    log.error("backup sync error", e);
                    retryTimes ++;
                }
            }
        }

    }

    private BaseClient getClient(){
        if(client == null){
            synchronized (this){
                if(client == null){
                    client = new BaseClient();
                }
            }
        }
        return client;
    }
}
