package com.jay.swarm.common.network.entity;

public class PacketTypes {
    /**
     * 上传请求
     * 该报文作用是向NameNode发起上传请求，让NameNode根据报文给出的文件信息选择DataNodes作为存储节点
     */
    public static final short UPLOAD_REQUEST = (short)1;

    /**
     * 回应报文
     * 网络中的response均为该类型，包括从NameNode到Client、NameNode到DataNode的心跳回复等。
     */
    public static final short RESPONSE = (short)2;

    /**
     * 提交命令报文
     * 该报文作用是让NameNode提交Client之前发起的修改命令，包括上传、删除。
     */
    public static final short SUBMIT = (short)3;

    /**
     * 上传文件
     * Client向DataNode上传完整文件的报文类型
     */
    public static final short UPLOAD_FILE = (short)4;

    /**
     * 上传分片
     * Client将大文件分片后向DataNode发送的分片报文
     */
    public static final short UPLOAD_SHARD = (short)5;

    /**
     * 最后分片
     */
    public static final short UPLOAD_LAST_SHARD = (short)6;

    /**
     * 心跳
     */
    public static final short HEART_BEAT = (short)7;

    /**
     * 下载请求
     * Client向NameNode发送下载文件请求，NameNode会去找到文件所在的DataNode
     */
    public static final short DOWNLOAD_REQUEST = (short)8;

    /**
     * 下载文件
     * 完整文件的传输报文类型
     */
    public static final short DOWNLOAD_FILE = (short)9;

    /**
     * 下载分片
     */
    public static final short DOWNLOAD_SHARD = (short)10;

    /**
     * 最后分片
     */
    public static final short DOWNLOAD_LAST_SHARD = (short)11;
}
