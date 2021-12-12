package com.jay.swarm.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 15:03
 */
@Builder
@Getter
@Setter
@ToString
public class StorageInfo {
    private String id;

    private String host;

    private int port;

    private long lastHeartBeatTime;

    private long usedStorage;

    private long freeStorage;


}
