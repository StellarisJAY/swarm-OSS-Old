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
 * @date 2021/12/09 15:05
 */
@Builder
@ToString
@Getter
public class MetaData {
    private String key;

    private String filename;

    private long uploadTime;

    private int size;

    private int backupCount;

    private List<String> storages;


}
