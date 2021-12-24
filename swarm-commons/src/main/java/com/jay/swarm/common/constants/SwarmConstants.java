package com.jay.swarm.common.constants;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * <p>
 *  常量
 * </p>
 *
 * @author Jay
 * @date 2021/12/10
 **/
public class SwarmConstants {
    /**
     * 默认心跳周期
     */
    public static final long DEFAULT_HEARTBEAT_PERIOD = 5 * 1000;

    /**
     * 默认编码 UTF-8
     */
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

    /**
     * 默认分片大小，4MB
     */
    public static final int DEFAULT_SHARD_SIZE = 1024 * 1024 * 4;
}
