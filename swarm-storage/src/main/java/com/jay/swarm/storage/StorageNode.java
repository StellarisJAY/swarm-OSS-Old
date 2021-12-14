package com.jay.swarm.storage;

import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.StorageInfo;
import com.jay.swarm.common.fs.FileInfoCache;
import com.jay.swarm.common.fs.locator.FileLocator;
import com.jay.swarm.common.fs.locator.Md5FileLocator;
import com.jay.swarm.common.network.BaseClient;
import com.jay.swarm.common.network.BaseServer;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.network.handler.FileTransferHandler;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.ScheduleUtil;
import com.jay.swarm.storage.handler.FileDownloadHandler;
import com.jay.swarm.storage.handler.StorageNodeHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/10
 **/
@Slf4j
public class StorageNode {
    private final BaseClient client;
    private final BaseServer server;
    private final Config config;
    private final String nodeId;

    private final String host;
    private final int port;
    private final FileInfoCache fileInfoCache;

    private static final String STORAGE_PATH = "D:/storage";

    public StorageNode(Config config) throws UnknownHostException {
        client = new BaseClient();
        server = new BaseServer();
        FileLocator locator = new Md5FileLocator(STORAGE_PATH);
        fileInfoCache = new FileInfoCache(locator);

        server.addHandler(new StorageNodeHandler(new FileTransferHandler(STORAGE_PATH, fileInfoCache), new FileDownloadHandler(fileInfoCache)));
        this.config = config;
        // 生成节点ID
        this.nodeId = UUID.randomUUID().toString();
        // 节点地址
        host = InetAddress.getLocalHost().getHostAddress();
        port = Integer.parseInt(config.get("server.port"));
    }

    /**
     * 初始化StorageNode
     */
    public void init() throws Exception {
        try{
            long initStart = System.currentTimeMillis();
            // 获取Overseer地址
            String host = config.get("overseer.host");
            String port = config.get("overseer.port");
            String serverPort = config.get("server.port");
            if(host == null || port == null || !port.matches("^[0-9]*$")){
                throw new RuntimeException("failed to start Storage Node, wrong Overseer Node Address");
            }
            if(serverPort == null || !serverPort.matches("^[0-9]*$")){
                throw new RuntimeException("failed to start storage Node serve, wrong port");
            }
            printBanner();
            checkStoragePath();

            server.bind(Integer.parseInt(serverPort));
            log.info("Storage Node server started, listening: {}", serverPort);
            // 连接Overseer
            client.connect(host, Integer.parseInt(port));
            // 注册节点
            registerNode();
            // 开启心跳
            startHeartBeat();
            log.info("successfully connected to Overseer Node at {}:{}", host, port);
            log.info("Storage Node init finished, time used: {}ms", (System.currentTimeMillis() - initStart));
        }catch (Exception e){
            log.error("failed to init Storage Node");
            throw e;
        }
    }

    /**
     * 检查存储目录，如果不存在将自动创建
     */
    public void checkStoragePath() {
        File file = new File(STORAGE_PATH);
        if(!file.exists() && !file.mkdirs()){
            throw new RuntimeException("unable to mkdir");
        }
    }

    /**
     * 注册StorageNode
     * @throws Exception exception
     */
    public void registerNode() throws Exception {
        try{
            long registerStart = System.currentTimeMillis();
            // 节点信息
            byte[] content = getStorageInfo();
            // 封装报文
            NetworkPacket packet = NetworkPacket.buildPacketOfType(PacketTypes.STORAGE_REGISTER, content);
            // 发送注册请求
            CompletableFuture<Object> future = client.sendAsync(packet);
            // 等待Overseer服务器回应
            Object response = future.get(10, TimeUnit.SECONDS);
            log.info("Storage Node {} registered to Overseer, time used: {}ms", nodeId, (System.currentTimeMillis() - registerStart));
        } catch (Exception e) {
            log.error("failed to register Storage Node");
            throw e;
        }
    }

    /**
     * 提交心跳任务
     * 用心跳包让Overseer知道当前节点存活
     */
    private void startHeartBeat(){
        Runnable heartBeatTask = ()->{
            try{
                // 节点状态
                byte[] content = getStorageInfo();
                // 封装心跳包
                NetworkPacket packet = NetworkPacket.buildPacketOfType(PacketTypes.HEART_BEAT, content);
                // 发送心跳包
                CompletableFuture<Object> future = client.sendAsync(packet);
                /*
                    等待Overseer的心跳回应
                 */
                Object response = future.get(SwarmConstants.DEFAULT_HEARTBEAT_PERIOD, TimeUnit.MILLISECONDS);
                log.info("heart-beat finished , status: {}", "success");
            } catch (ExecutionException | InterruptedException e) {
                log.error("heart-beat ends with exception", e);
            } catch (TimeoutException e) {
                // 等待Overseer回应超时
                log.error("heart-beat timeout", e);
            }
        };
        // 提交心跳周期任务，延迟一个周期开始
        ScheduleUtil.scheduleAtFixedRate(heartBeatTask, SwarmConstants.DEFAULT_HEARTBEAT_PERIOD, SwarmConstants.DEFAULT_HEARTBEAT_PERIOD, TimeUnit.MILLISECONDS);
        log.info("heart-beat schedule done");
    }

    /**
     * 获取节点信息，并序列化为byte
     * @return byte[]
     */
    private byte[] getStorageInfo(){
        File storageDir = new File(STORAGE_PATH);
        StorageInfo storageInfo = StorageInfo.builder().host(host)
                .port(port)
                .usedStorage(storageDir.getTotalSpace())
                .freeStorage(storageDir.getFreeSpace())
                .id(nodeId)
                .build();
        log.info("storage node status: {}", storageInfo.toString());
        Serializer serializer = new ProtoStuffSerializer();
        return serializer.serialize(storageInfo, StorageInfo.class);
    }
    private void printBanner(){
        try(InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("banner")){
            if(inputStream != null){
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader reader = new BufferedReader(inputStreamReader);
                String line;
                while((line = reader.readLine()) != null){
                    System.out.println(line);
                }
                reader.close();
                inputStreamReader.close();
            }
        } catch (IOException ignored) {

        }
    }

    public static void main(String[] args) throws Exception {
        Config config = new Config("D:/storage.properties");
        StorageNode storageNode = new StorageNode(config);
        storageNode.init();
    }
}
