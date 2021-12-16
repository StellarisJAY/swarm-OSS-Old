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
 *
 *  <h1>备份接力</h1>
 *  客户端只从Overseer分配的存储节点选择一个节点作为上传点<br>
 *  剩下的存储点备份数据由存储节点自己传递<br>
 *  这里使用了一种接力的方式来分散备份的开销。<br>
 *  <br><br>
 *  <h2>接力</h2>
 *  每个存储节点收到文件数据的同时，也会收到Overseer选择的其他节点<br>
 *  当前节点成功保存文件副本后，会从其他节点中选择一个，然后将文件数据发送出去。<br>
 *  每个收到的节点都将数据存储再转发给下一个节点，就像接力赛一样。<br>
 *  <br><br>
 *  <h2>断路</h2>
 *  有可能出现接力链上的某个节点掉线<br>
 *  所以每个转发节点将在目标节点中轮询，直到有一个节点成功接收。<br>
 *  可以理解为这个接力顺序是由下一个选手的可达性决定的，不可达的不会作为下一棒，而会被转移到下一棒接力。<br>
 *  <br><br>
 *  接力到最后可能遇到一两个不可达节点，这时应该向Overseer请求新的存储节点。<br>
 *  <br><br>
 *  最极端的情况是所有的节点都掉线了，那么数据将无法再完成备份，不过这种情况出现的可能极低。<br>
 *
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
            /*
                轮询尝试方式向一个节点发送备份
                成功收到的节点作为下一个接力节点做相同的轮询发送。
                直到所有节点都收到
             */
            int maxRetryTimes = storages.size();
            long start = System.currentTimeMillis();
            int retryTimes = 0;
            // 尝试同步备份
            while(retryTimes < maxRetryTimes){
                // 选第一个作为下一棒接力节点
                StorageInfo targetNode = storages.remove(0);
                try{
                    // 目标节点地址
                    String host = targetNode.getHost();
                    int port = targetNode.getPort();

                    // 封装HEAD报文
                    byte[] serializedHead = serializer.serialize(fileInfo, FileInfo.class);
                    NetworkPacket head = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_HEAD, serializedHead);

                    // 建立连接
                    BaseClient client = getClient();
                    client.connect(host, port);
                    // 发送HEAD，等待回复
                    NetworkPacket headResponse = (NetworkPacket) client.sendAsync(head).get();
                    if(headResponse.getType() == PacketTypes.ERROR){
                        throw new RuntimeException(new String(headResponse.getContent(), SwarmConstants.DEFAULT_CHARSET));
                    }

                    // 发送文件数据
                    ShardedFileSender shardedFileSender = new ShardedFileSender(client, serializer, new DefaultFileTransferCallback());
                    shardedFileSender.send(new File(path), fileInfo.getFileId());

                    // 封装END报文
                    FileUploadEnd uploadEnd = FileUploadEnd.builder()
                            .fileId(fileInfo.getFileId())
                            .otherStorages(storages)
                            .build();
                    byte[] serializedEnd = serializer.serialize(uploadEnd, FileUploadEnd.class);
                    NetworkPacket end = NetworkPacket.buildPacketOfType(PacketTypes.TRANSFER_FILE_END, serializedEnd);
                    // 发送END，等待最终返回
                    NetworkPacket finalResponse = (NetworkPacket) client.sendAsync(end).get();

                    if(finalResponse.getType() == PacketTypes.SUCCESS){
                        log.info("backup synchronized to peer storage node, time used {} ms", (System.currentTimeMillis() - start));
                        // 备份成功，结束循环
                        break;
                    }
                    else{
                        storages.add(targetNode);
                        retryTimes++;
                    }
                }catch (Exception e){
                    log.error("backup sync error", e);
                    retryTimes++;
                    storages.add(targetNode);
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
