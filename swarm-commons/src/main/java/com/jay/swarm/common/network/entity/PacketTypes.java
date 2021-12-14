package com.jay.swarm.common.network.entity;

/**
 * <p>
 *     报文类型
 * </p>
 * @author Jay
 */
public class PacketTypes {
    /**
     * 心跳
     */
    public static final short HEART_BEAT = (short)1;

    /**
     * 存储节点注册
     */
    public static final short STORAGE_REGISTER = (short)2;

    /**
     * 存储节点注册回复
     */
    public static final short STORAGE_REGISTER_RESPONSE = (short)3;

    /**
     * 传输文件头
     */
    public static final short TRANSFER_FILE_HEAD = (short)4;

    /**
     * 传输文件分片
     */
    public static final short TRANSFER_FILE_BODY = (short)5;

    /**
     * 传输文件结束
     */
    public static final short TRANSFER_FILE_END = (short)6;

    /**
     * 传输回复
     */
    public static final short TRANSFER_RESPONSE = (short)7;

    /**
     * 下载请求
     */
    public static final short DOWNLOAD_REQUEST = (short)8;

    public static final short TRANSFER_FILE_HEAD_D = (short)9;

    public static final short ERROR = (short)10;

}
