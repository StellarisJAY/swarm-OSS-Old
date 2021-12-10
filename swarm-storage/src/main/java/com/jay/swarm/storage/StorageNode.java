package com.jay.swarm.storage;

import com.jay.swarm.common.config.Config;
import com.jay.swarm.common.constants.SwarmConstants;
import com.jay.swarm.common.entity.Storage;
import com.jay.swarm.common.network.BaseClient;
import com.jay.swarm.common.network.ResponseWaitSet;
import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.entity.PacketTypes;
import com.jay.swarm.common.serialize.ProtoStuffSerializer;
import com.jay.swarm.common.serialize.Serializer;
import com.jay.swarm.common.util.ScheduleUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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
    private final Config config;
    private final String nodeId;

    private final String host;
    private final int port;

    public StorageNode(Config config) throws UnknownHostException {
        client = new BaseClient();
        this.config = config;
        this.nodeId = UUID.randomUUID().toString();
        host = InetAddress.getLocalHost().getHostName();
        port = Integer.parseInt(config.get("server.port"));
    }

    public void init(){
        try{
            long initStart = System.currentTimeMillis();
            String host = config.get("overseer.host");
            String port = config.get("overseer.port");
            if(host == null || port == null || !port.matches("^[0-9]*$")){
                throw new RuntimeException("failed to start Storage Node, wrong Overseer Node Address");
            }
            printBanner();
            client.connect(host, Integer.parseInt(port));
            log.info("successfully connected to Overseer Node at {}:{}", host, port);
            log.info("Storage Node init finished, time used: {}ms", (System.currentTimeMillis() - initStart));
        }catch (Exception e){
            log.error("failed to init Storage Node");
            throw e;
        }
    }

    public void registerNode() throws Exception {
        try{
            long registerStart = System.currentTimeMillis();
            byte[] content = getStorageInfo();
            NetworkPacket packet = NetworkPacket.builder().content(content).type(PacketTypes.STORAGE_REGISTER).build();

            CompletableFuture<Object> future = client.sendAsync(packet);
            Object response = future.get(10, TimeUnit.SECONDS);
            log.info("Storage Node {} registered to Overseer, time used: {}ms", nodeId, (System.currentTimeMillis() - registerStart));
        } catch (Exception e) {
            log.error("failed to register Storage Node");
            throw e;
        }
    }

    private void startHeartBeat(){
        Runnable heartBeatTask = ()->{
            try{
                byte[] content = getStorageInfo();
                NetworkPacket packet = NetworkPacket.buildPacketOfType(PacketTypes.HEART_BEAT, content);
                CompletableFuture<Object> future = client.sendAsync(packet);
                Object response = future.get(SwarmConstants.DEFAULT_HEARTBEAT_PERIOD, TimeUnit.MILLISECONDS);
                log.info("heart-beat finished , status: {}", "success");
            } catch (ExecutionException | InterruptedException e) {
                log.error("heart-beat ends with exception", e);
            } catch (TimeoutException e) {
                log.error("heart-beat timeout", e);
            }
        };
        ScheduleUtil.scheduleAtFixedRate(heartBeatTask, SwarmConstants.DEFAULT_HEARTBEAT_PERIOD, SwarmConstants.DEFAULT_HEARTBEAT_PERIOD, TimeUnit.MILLISECONDS);
        log.info("heart-beat schedule done");
    }

    private byte[] getStorageInfo(){
        Storage storage = Storage.builder().host(host)
                .port(port)
                .diskUsagePercent(100.0)
                .id(nodeId)
                .build();
        Serializer serializer = new ProtoStuffSerializer();
        return serializer.serialize(storage, Storage.class);
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

    public static void main(String[] args) throws Exception {
        Config config = new Config("D:/storage.properties");
        StorageNode storageNode = new StorageNode(config);
        storageNode.init();
        storageNode.registerNode();
        storageNode.startHeartBeat();
    }
}
