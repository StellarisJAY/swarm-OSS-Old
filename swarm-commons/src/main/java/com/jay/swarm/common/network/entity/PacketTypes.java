package com.jay.swarm.common.network.entity;

/**
 * @author Jay
 */
public class PacketTypes {
    public static final short HEART_BEAT = (short)1;

    public static final short STORAGE_REGISTER = (short)2;

    public static final short STORAGE_REGISTER_RESPONSE = (short)3;

    public static final short TRANSFER_FILE_HEAD = (short)4;

    public static final short TRANSFER_FILE_BODY = (short)5;

    public static final short TRANSFER_FILE_END = (short)6;

    public static final short TRANSFER_RESPONSE = (short)7;

}
