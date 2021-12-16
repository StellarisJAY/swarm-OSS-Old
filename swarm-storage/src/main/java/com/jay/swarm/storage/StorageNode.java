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
import com.jay.swarm.common.util.StringUtils;
import com.jay.swarm.storage.handler.FileDownloadHandler;
import com.jay.swarm.storage.handler.StorageNodeHandler;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Inet4Address;
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
    private String nodeId;
    private final Serializer serializer;
    private final String host;
    private final int port;
    private final FileInfoCache fileInfoCache;
    private final String STORAGE_PATH;

    private final FileLocator locator;

    private static final String DEFAULT_NODE_ID_PATH = "node_id.info";
    private static final String DEFAULT_STORAGE_PATH = "D:/storage";

    public StorageNode(Config config) throws UnknownHostException {
        this.config = config;
        // 配置文件加载存储根目录
        String storagePath = config.get("storage.root");
        this.STORAGE_PATH = StringUtils.isEmpty(storagePath) ? DEFAULT_STORAGE_PATH : storagePath;
        // 生成节点ID
        this.nodeId = UUID.randomUUID().toString();
        client = new BaseClient();
        server = new BaseServer();
        // 默认序列化工具
        this.serializer = new ProtoStuffSerializer();
        // 文件定位器
        locator = new Md5FileLocator(this.STORAGE_PATH);
        // 文件信息缓存
        fileInfoCache = new FileInfoCache(locator);


        // 节点地址
        host = Inet4Address.getLocalHost().getHostAddress();
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
            // 检查存储路径
            checkStoragePath();

            // 连接Overseer
            client.connect(host, Integer.parseInt(port));
            log.info("successfully connected to Overseer Node at {}:{}", host, port);
            // 注册节点
            registerNode();
            // 开启心跳
            startHeartBeat();
            // 服务器添加存储节点处理器，开启服务器
            // 传输处理器
            FileTransferHandler transferHandler = new FileTransferHandler(fileInfoCache);
            // 下载处理器
            FileDownloadHandler downloadHandler = new FileDownloadHandler(fileInfoCache, serializer);
            server.addHandler(new StorageNodeHandler(nodeId, transferHandler, downloadHandler, locator, serializer, client, fileInfoCache));
            server.bind(Integer.parseInt(serverPort));
            log.info("Storage Node server started, listening: {}", serverPort);
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
        File file = new File(DEFAULT_STORAGE_PATH);
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
            StorageInfo storageInfo = getStorageInfo();
            // 加载ID
            storageInfo.setId(getStorageNodeId());
            byte[] content = serializer.serialize(storageInfo, StorageInfo.class);
            // 封装报文
            NetworkPacket packet = NetworkPacket.buildPacketOfType(PacketTypes.STORAGE_REGISTER, content);
            // 发送注册请求
            CompletableFuture<Object> future = client.sendAsync(packet);
            // 等待Overseer服务器回应
            NetworkPacket response = (NetworkPacket) future.get(10, TimeUnit.SECONDS);
            if(response.getType() == PacketTypes.ERROR){
                throw new RuntimeException(new String(response.getContent(), SwarmConstants.DEFAULT_CHARSET));
            }
            else{
                // 由Overseer分配的ID
                this.nodeId = new String(response.getContent(), SwarmConstants.DEFAULT_CHARSET);
                // 保存节点ID
                saveNodeId(nodeId);
                log.info("storage node successfully registered to Overseer");
            }
        } catch (Exception e) {
            log.error("failed to register Storage Node");
            throw e;
        }
    }



    private void saveNodeId(String nodeId) throws Exception{
        try(OutputStream outputStream = new FileOutputStream(DEFAULT_NODE_ID_PATH)){
            OutputStreamWriter out = new OutputStreamWriter(outputStream);
            BufferedWriter writer = new BufferedWriter(out);
            writer.write(nodeId);
            writer.newLine();
            writer.close();
            out.close();
        }catch (Exception e){
            log.info("cant save node id");
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
                StorageInfo storageInfo = getStorageInfo();
                storageInfo.setId(this.nodeId);
                byte[] content = serializer.serialize(storageInfo, StorageInfo.class);
                // 封装心跳包
                NetworkPacket packet = NetworkPacket.buildPacketOfType(PacketTypes.HEART_BEAT, content);
                // 发送心跳包
                CompletableFuture<Object> future = client.sendAsync(packet);
                /*
                    等待Overseer的心跳回应
                 */
                Object response = future.get(SwarmConstants.DEFAULT_HEARTBEAT_PERIOD, TimeUnit.MILLISECONDS);
                log.debug("heart-beat finished , status: {}", "success");
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
     * @return StorageInfo
     */
    private StorageInfo getStorageInfo(){
        File storageDir = new File(DEFAULT_STORAGE_PATH);
        return StorageInfo.builder().host(host)
                .port(port)
                .usedStorage(storageDir.getTotalSpace())
                .freeStorage(storageDir.getFreeSpace())
                .build();
    }

    private String getStorageNodeId() throws Exception{
        File file = new File(DEFAULT_NODE_ID_PATH);
        if(!file.exists() || file.isDirectory()){
            return null;
        }
        try(InputStream inputStream = new FileInputStream(file)){
            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader reader = new BufferedReader(inputStreamReader);
            String line = reader.readLine();
            reader.close();
            inputStreamReader.close();
            return line;
        }catch (Exception e){
            log.info("cant load node id from file");
            throw e;
        }
    }

    private void printBanner(){
        try(InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("banner")){
            if(inputStream != null){
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader reader = new BufferedReader(inputStreamReader);
                String line;
                while((line = reader.readLine()) != null){
                    System.out.format("\33[33;1m %s%n", line);
                }
                reader.close();
                inputStreamReader.close();
            }
        } catch (IOException ignored) {

        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 1){
            throw new RuntimeException("no enough args to start storage node");
        }
        // arg[0] 作为配置文件路径
        Config config = new Config(args[0]);
        StorageNode storageNode = new StorageNode(config);
        storageNode.init();
    }
}
