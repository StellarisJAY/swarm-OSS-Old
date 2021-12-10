package com.jay.swarm.overseer;

import com.jay.swarm.common.network.BaseServer;
import com.jay.swarm.overseer.config.OverseerConfig;
import com.jay.swarm.overseer.handler.PacketHandler;
import com.jay.swarm.overseer.meta.MetaDataManager;
import com.jay.swarm.overseer.meta.Persistence;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 16:10
 */
public class Overseer {

    private final MetaDataManager metaDataManager;

    private final BaseServer overseerServer;

    private final Persistence persistence;

    private final OverseerConfig config;

    public Overseer(OverseerConfig config) {
        this.config = config;
        metaDataManager = new MetaDataManager();
        overseerServer = new BaseServer();
        persistence = new Persistence(metaDataManager);
    }

    public void init(){
        // 添加Handler
        overseerServer.addHandler(new PacketHandler());
        // 启动持久化任务
        persistence.init(30 * 1000);
    }

    public static void main(String[] args) {
        Overseer overseer = new Overseer(new OverseerConfig("D:/overseer.properties"));
        overseer.init();
    }

}
