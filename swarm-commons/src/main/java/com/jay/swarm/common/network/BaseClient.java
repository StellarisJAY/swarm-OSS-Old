package com.jay.swarm.common.network;

import com.jay.swarm.common.network.entity.NetworkPacket;
import com.jay.swarm.common.network.handler.BaseClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>
 *  基础客户端
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 15:27
 */
@Slf4j
public class BaseClient {

    private final BaseChannelInitializer channelInitializer = new BaseChannelInitializer();
    /**
     * channelMap，保留向StorageNode的连接
     */
    private final static Map<String, Channel> CHANNEL_MAP = new ConcurrentHashMap<>(256);

    private final EventLoopGroup group = new NioEventLoopGroup();
    /**
     * 当前连接的channel
     */
    private Channel currentChannel;
    /**
     * 等待队列
     */
    private final ResponseWaitSet responseWaitSet = new ResponseWaitSet();

    private final AtomicInteger idProvider = new AtomicInteger(1);


    public BaseClient() {
    }

    public void addHandler(ChannelHandler handler){
        channelInitializer.addHandler(handler);
    }

    public void addHandlers(Collection<ChannelHandler> handlers){
        channelInitializer.addHandlers(handlers);
    }

    /**
     * 建立连接，并缓存channel
     * @param host host
     * @param port port
     * @return Channel
     * @throws ConnectException 连接失败异常
     */
    public Channel connect(String host, int port) throws ConnectException {
        try{
            String key = host + ":" + port;
            // CHANNEL_MAP中找到channel
            Channel channel = CHANNEL_MAP.get(key);
            // 连接不存在 或 无法使用
            if(channel == null || !channel.isActive()){
                Bootstrap bootstrap = new Bootstrap()
                        .group(group)
                        .channel(NioSocketChannel.class)
                        .handler(channelInitializer)
                        .remoteAddress(new InetSocketAddress(host, port));
                addHandler(new BaseClientHandler(responseWaitSet));
                // 建立连接
                ChannelFuture channelFuture = bootstrap.connect().sync();
                channel = channelFuture.channel();
                CHANNEL_MAP.put(key, channel);
            }
            return channel;
        } catch (Exception e) {
            // 关闭线程组
            group.shutdownGracefully();
            if(log.isDebugEnabled()){
                log.debug("connection refused {}", host + ":" + port);
            }
            throw new ConnectException("connection refused by " + host + ":" + port);
        }
    }

    /**
     * 发送报文
     * @param host 目标地址
     * @param port 目标端口
     * @param packet 报文
     * @return CompletableFuture
     * @throws ConnectException 连接异常
     */
    public CompletableFuture<Object> sendAsync(String host, int port, NetworkPacket packet) throws ConnectException {
        // 建立连接
        Channel channel = connect(host, port);
        CompletableFuture<Object> result = new CompletableFuture<>();
        packet.setId(idProvider.getAndIncrement());
        // 缓存future
        responseWaitSet.addWaiter(packet.getId(), result);
        // 发送报文
        channel.writeAndFlush(packet);
        return result;
    }

    public void shutdown(){
        // 关闭当前连接
        if(currentChannel != null){
            currentChannel.close();
        }
        // 关闭CHANNEL_MAP中的连接
        CHANNEL_MAP.values().forEach(ChannelOutboundInvoker::close);
        // 关闭线程组
        group.shutdownGracefully();
    }
}
