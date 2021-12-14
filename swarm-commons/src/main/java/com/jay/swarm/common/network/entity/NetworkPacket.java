package com.jay.swarm.common.network.entity;

import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * <p>
 * Swarm-dfs网络通信基础报文。
 *
 * 0    1    2    3    4    5    6    7    8    9    10   11   12
 * +----+----+----+----+----+----+----+----+----+----+----+----+
 * | m_number|       length      |  type   |      id           |
 * +---------+-------------------+---------+-------------------+
 * |                                                           |
 * |                    data                                   |
 * |                                                           |
 * |                                                           |
 * +-----------------------------------------------------------+
 *
 * m_number：魔数，2字节，5df5，用于识别报文。
 * length：报文总长度，首部12字节 + data长度。
 * type：报文类型，2字节。
 * id：报文ID，4字节。
 * data：数据部分，序列化后的数据或者是文件数据
 * </p>
 *
 * @author Jay
 * @date 2021/12/8
 **/
@Builder
@Getter
@ToString
public class NetworkPacket {
    private int length;
    private short type;
    private byte[] content;
    private int id;

    /**
     * 首部Magic Number
     */
    public static final short MAGIC_NUMBER = (short)0x5DF5;
    /**
     * 首部长度
     */
    public static final int HEADER_LENGTH = 12;

    /**
     * 报文最大长度 = 首部长度 + 最大文件块大小 (128M)
     */
    public static final int MAX_PACKET_LENGTH = HEADER_LENGTH + 128 * 1024 * 1024;


    /**
     * 将packet对象编码写入ByteBuf
     * @param packet 数据包
     * @param buffer ByteBuf
     */
    public static void encode(NetworkPacket packet, ByteBuf buffer){
        buffer.writeShort(MAGIC_NUMBER);
        buffer.writeInt(packet.getLength());
        buffer.writeShort(packet.getType());
        buffer.writeInt(packet.getId());
        // 有content数组
        if(packet.content != null){
            buffer.writeBytes(packet.content);
        }
    }

    /**
     * 解码，将ByteBuf解析出Packet
     * @param buffer ByteBuf
     * @return NetworkPacket
     */
    public static NetworkPacket decode(ByteBuf buffer){
        // 报文小于header长度或者魔数错误
        if(buffer.readableBytes() < HEADER_LENGTH || !checkMagicNumber(buffer)){
            throw new RuntimeException("invalid network packet");
        }
        // 读取长度
        int length = buffer.readInt();
        // 读取类型
        short type = buffer.readShort();
        // 读取ID
        int id = buffer.readInt();

        // 总长度-头部长度 = 数据部分长度
        int contentLength = length - HEADER_LENGTH;
        // ByteBuf中剩余字节数不够
        if(contentLength > buffer.readableBytes()){
            throw new RuntimeException("packet format error");
        }
        // 读取content
        byte[] content = new byte[contentLength];
        buffer.readBytes(content);

        buffer.release();

        return builder().length(length)
                .type(type)
                .id(id)
                .content(content)
                .build();
    }

    /**
     * 检查魔数
     * 用于判断收到的是否是SDFS的报文
     * @param buffer ByteBuf
     * @return boolean
     */
    private static boolean checkMagicNumber(ByteBuf buffer){
        short number = buffer.readShort();
        return number == MAGIC_NUMBER;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public static NetworkPacket buildPacketOfType(short packetType, byte[] content){
        return NetworkPacket.builder()
                .type(packetType)
                .content(content)
                .build();
    }
}
