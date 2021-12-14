package com.jay.swarm.common.fs;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * <p>
 *  文件分片实体
 * </p>
 *
 * @author Jay
 * @date 2021/12/13
 **/
@Builder
@Getter
@ToString
public class FileShard {
    /**
     * 文件ID
     */
    private String fileId;
    /**
     * 分片内容
     */
    private byte[] content;
}
