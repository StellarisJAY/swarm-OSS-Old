package com.jay.swarm.common.entity;

import java.util.Map;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 15:06
 */
public class Bucket {
    private String id;

    private String name;

    private long createTime;

    private Map<String, MetaData> objectCache;
}
