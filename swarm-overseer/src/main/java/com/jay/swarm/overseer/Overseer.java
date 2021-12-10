package com.jay.swarm.overseer;

import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.network.BaseServer;
import com.jay.swarm.overseer.config.OverseerConfig;
import com.jay.swarm.overseer.handler.PacketHandler;
import com.jay.swarm.overseer.meta.MetaDataManager;
import com.jay.swarm.overseer.meta.Persistence;
import com.jay.swarm.overseer.storage.StorageManager;
import lombok.extern.slf4j.Slf4j;

import java.io.*;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 16:10
 */
@Slf4j
public class Overseer {

    private final MetaDataManager metaDataManager;

    private final BaseServer overseerServer;

    private final Persistence persistence;

    private final StorageManager storageManager;

    private final Config config;

    public Overseer(Config config) {
        this.config = config;
        metaDataManager = new MetaDataManager();
        storageManager = new StorageManager(config);
        overseerServer = new BaseServer();
        persistence = new Persistence(metaDataManager);
    }

    public void init(){
        long initStart = System.currentTimeMillis();
        String port = config.get("server.port");
        if(port == null || !port.matches("^[0-9]*$")){
            throw new RuntimeException("failed to start Overseer Node, wrong port config");
        }
        printBanner();
        // 添加Handler
        overseerServer.addHandler(new PacketHandler(storageManager, metaDataManager));
        // 启动持久化任务
        persistence.init(30 * 1000);
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
        Overseer overseer = new Overseer(new Config("D:/overseer.properties"));
        overseer.init();
    }

}
