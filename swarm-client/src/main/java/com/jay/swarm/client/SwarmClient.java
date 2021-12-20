package com.jay.swarm.client;

import com.jay.swarm.client.download.DownloadHelper;
import com.jay.swarm.client.handler.SwarmClientHandler;
import com.jay.swarm.client.upload.UploadHelper;
import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.entity.DownloadResponse;
import com.jay.swarm.common.network.BaseClient;
import com.jay.swarm.common.network.callback.DefaultFileTransferCallback;
import com.jay.swarm.common.network.callback.FileTransferCallback;
import com.jay.swarm.common.network.handler.FileTransferHandler;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import lombok.extern.slf4j.Slf4j;

import java.net.ConnectException;

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
    private final BaseClient overseerClient;
    private final BaseClient storageClient;
    private final Serializer serializer;
    private final Config config;
    private final UploadHelper uploadHelper;
    private final DownloadHelper downloadHelper;
    private final static String DOWNLOAD_DIR = "D:/swarm/downloads";
    private static final int DEFAULT_BACKUP_COUNT = 2;

    public SwarmClient(Config config){
        this.config = config;
        this.overseerClient = new BaseClient();
        this.storageClient = new BaseClient();
        this.serializer = new ProtoStuffSerializer();
        FileTransferHandler transferHandler = new FileTransferHandler();
        SwarmClientHandler clientHandler = new SwarmClientHandler(transferHandler, serializer, DOWNLOAD_DIR);
        this.overseerClient.addHandler(clientHandler);
        this.storageClient.addHandler(clientHandler);
        this.uploadHelper = new UploadHelper(overseerClient, storageClient, serializer, config);
        this.downloadHelper = new DownloadHelper(overseerClient, storageClient, serializer, config);
        init();
    }

    private void init(){
        try {
            this.uploadHelper.init();
        } catch (Exception e) {
            e.printStackTrace();
            this.shutdownGracefully();
        }
    }

    /**
     * 上传文件
     * @param path 文件路径
     * @param callback 上传过程回调
     * @return 文件ID
     */
    public String upload(String path, FileTransferCallback callback) throws Exception {
        return uploadHelper.upload(path, DEFAULT_BACKUP_COUNT, callback);
    }

    /**
     * 上传文件
     * @param path 文件路径
     * @return 文件ID
     * @throws Exception Exception
     */
    public String upload(String path) throws Exception{
        return uploadHelper.upload(path, DEFAULT_BACKUP_COUNT, new DefaultFileTransferCallback());
    }

    /**
     * 上传文件，使用默认的callback
     * @param path 文件路径
     * @param backupCount 备份数量
     * @return 文件ID
     * @throws Exception Exception
     */
    public String upload(String path, int backupCount) throws Exception{
        return uploadHelper.upload(path, backupCount, new DefaultFileTransferCallback());
    }

    public void download(String fileId){
       try{
           DownloadResponse response = downloadHelper.sendDownloadRequest(fileId);
           downloadHelper.pullData(response);
       }catch (ConnectException e){
           log.error("unable to reach target Node, please check Node status, error:", e);
       }catch (Exception e){
           log.error("download process error: ", e);
       }
    }



    public void shutdownGracefully(){
        overseerClient.shutdown();
        storageClient.shutdown();
    }
}
