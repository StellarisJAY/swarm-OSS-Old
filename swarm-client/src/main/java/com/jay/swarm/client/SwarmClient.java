package com.jay.swarm.client;
import com.jay.swarm.client.network.SwarmNettyClient;
import com.jay.swarm.client.network.UnfinishedRequestContainer;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/8
 **/
public class SwarmClient {

    private final SwarmNettyClient client;
    private final UnfinishedRequestContainer unfinishedRequestContainer;

    public SwarmClient() {
        // 创建未完成请求容器
        unfinishedRequestContainer = new UnfinishedRequestContainer();
        // 创建Swarm-dfs网络客户端
        client = new SwarmNettyClient(unfinishedRequestContainer);
    }
}
