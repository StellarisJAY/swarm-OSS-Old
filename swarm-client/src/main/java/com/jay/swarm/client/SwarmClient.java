package com.jay.swarm.client;

import com.jay.swarm.client.handler.SwarmClientHandler;
import com.jay.swarm.client.upload.UploadHelper;
import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.network.BaseClient;
import com.jay.swarm.common.network.callback.DefaultFileTransferCallback;
import com.jay.swarm.common.network.callback.FileTransferCallback;
import com.jay.swarm.common.network.handler.FileTransferHandler;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  SWARM-DFS 客户端
 *  使用该客户端对象向SWARM存储系统发送命令
 * </p>
 *
 * @author Jay
 * @date 2021/12/14 13:23
 */
@Slf4j
public class SwarmClient {
    private final BaseClient client;
    private final Serializer serializer;
    private final Config config;
    private volatile UploadHelper uploadHelper;

    private final static String DOWNLOAD_DIR = "D:/swarm/downloads";
    public SwarmClient(Config config){
        this.config = config;
        this.client = new BaseClient();
        this.serializer = new ProtoStuffSerializer();
        FileTransferHandler transferHandler = new FileTransferHandler();
        SwarmClientHandler clientHandler = new SwarmClientHandler(transferHandler, serializer, DOWNLOAD_DIR);
        this.client.addHandler(clientHandler);
    }

    /**
     * 上传文件
     * @param path 文件路径
     * @param callback 上传过程回调
     * @return 文件ID
     */
    public String upload(String path, FileTransferCallback callback) throws Exception {
        // 上传功能懒加载，避免启动时创建客户端连接
        if(uploadHelper == null){
            synchronized (this){
                if(uploadHelper == null){
                    uploadHelper = new UploadHelper(client, serializer, config);
                }
            }
        }
        return uploadHelper.upload(path, callback);
    }

    public static void main(String[] args) throws Exception {
        SwarmClient swarmClient = new SwarmClient(new Config(args[0]));
        swarmClient.upload("D:/01.pdf", new DefaultFileTransferCallback());
    }
}
