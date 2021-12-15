package com.jay.swarm.common.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/15 10:46
 */
@Builder
@Getter
@ToString
public class FileMetaStorage {
    private String fileId;
    private String storageId;

}
