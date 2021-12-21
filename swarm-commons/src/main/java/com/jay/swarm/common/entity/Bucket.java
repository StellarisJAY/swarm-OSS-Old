package com.jay.swarm.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.util.List;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 15:06
 */
@Builder
@Getter
@ToString
public class Bucket {
    /**
     * 桶ID
     */
    private String id;

    /**
     * 桶名称
     */
    private String name;

    /**
     * 创建时间
     */
    private long createTime;

    private long ownerId;

    private short accessibility;

    /**
     * 对象集合
     */
    private List<String> objectIds;
}
