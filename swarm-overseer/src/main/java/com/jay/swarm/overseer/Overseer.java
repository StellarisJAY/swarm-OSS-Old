package com.jay.swarm.overseer;

import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.network.BaseServer;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.overseer.handler.PacketHandler;
import com.jay.swarm.overseer.meta.MetaDataManager;
import com.jay.swarm.overseer.meta.Persistence;
import com.jay.swarm.overseer.storage.StorageManager;
import lombok.extern.slf4j.Slf4j;

import java.io.*;

/**
 * <p>
 *  OVERSEER节点主类
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 16:10
 */
@Slf4j
public class Overseer {
    /**
     * 元数据管理器
     */
    private final MetaDataManager metaDataManager;

    /**
     * Overseer网络服务器
     */
    private final BaseServer overseerServer;

    /**
     * 持久化工具
     */
    private final Persistence persistence;

    /**
     * 存储节点管理器
     */
    private final StorageManager storageManager;

    private final Serializer serializer;

    private final Config config;

    public Overseer(Config config) {
        this.config = config;
        this.serializer = new ProtoStuffSerializer();
        metaDataManager = new MetaDataManager();
        storageManager = new StorageManager(config);
        overseerServer = new BaseServer();
        persistence = new Persistence(metaDataManager, config, serializer);
    }

    /**
     * 初始化Overseer
     */
    public void init(){
        long initStart = System.currentTimeMillis();
        // 从配置文件获取port
        String port = config.get("server.port");
        if(port == null || !port.matches("^[0-9]*$")){
            throw new RuntimeException("failed to start Overseer Node, wrong port config");
        }
        printBanner();
        // 添加Handler
        overseerServer.addHandler(new PacketHandler(storageManager, metaDataManager, serializer));
        // 启动持久化任务
        persistence.init(30 * 1000);
        // 开启Overseer服务器
        overseerServer.bind(Integer.parseInt(port));
        log.info("Overseer server started, listening port {}", port);
        log.info("Overseer Node init finished, time used: {}ms", (System.currentTimeMillis() - initStart));
    }

    private void printBanner(){
        try(InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("banner")){
            if(inputStream != null){
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader reader = new BufferedReader(inputStreamReader);
                String line = null;
                while((line = reader.readLine()) != null){
                    System.out.println(line);
                }
                reader.close();
                inputStreamReader.close();
            }
        } catch (IOException ignored) {

        }
    }

    public static void main(String[] args) {
        if(args.length < 1){
            throw new IllegalArgumentException("no enough args to start overseer node");
        }
        Overseer overseer = new Overseer(new Config(args[0]));
        overseer.init();
    }
}
