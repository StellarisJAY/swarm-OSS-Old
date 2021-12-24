package com.jay.swarm.common.network.entity;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 * Swarm-oss网络通信基础报文。
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
@Slf4j
public class NetworkPacket {
    private int length;
    private short type;
    private byte[] content;
    private int id;
    private ByteBuf header;
    private ByteBuf data;

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

    public static ByteBuf buildPacketOfType(short type, ByteBuf data){
        ByteBuf header = header(0, type, data.readableBytes());
        return combine(header, data);
    }

    /**
     * 生成HEADER
     * @param id id
     * @param type 类型
     * @param dataLength 数据长度
     * @return ByteBuf
     */
    public static ByteBuf header(int id, short type, int dataLength){
        ByteBuf header = Unpooled.buffer();
        header.writeShort(MAGIC_NUMBER);
        header.writeInt(HEADER_LENGTH + dataLength);
        header.writeShort(type);
        header.writeInt(id);
        return header;
    }

    /**
     * 合并HEADER和DATA
     * @param header header
     * @param data data
     * @return ByteBuf
     */
    public static ByteBuf combine(ByteBuf header, ByteBuf data){
        CompositeByteBuf compositeBuffer = Unpooled.compositeBuffer();
        compositeBuffer.addComponent(true, header);
        compositeBuffer.addComponent(true, data);
        return compositeBuffer;
    }

    /**
     * 解码，将ByteBuf解析出Packet
     * @param buffer ByteBuf
     * @return NetworkPacket
     */
    public static NetworkPacket decode(ByteBuf buffer){
        // 报文小于header长度或者魔数错误
        if(buffer.readableBytes() < HEADER_LENGTH){
            throw new RuntimeException("invalid network packet");
        }
        // 切割buffer，得到HEADER和DATA
        ByteBuf header = buffer.slice(0, HEADER_LENGTH);

        if(!checkMagicNumber(header)){
            throw new RuntimeException("invalid network packet");
        }
        ByteBuf data = buffer.slice(HEADER_LENGTH, buffer.readableBytes() - HEADER_LENGTH);
        // 读取长度
        int length = header.readInt();
        // 读取类型
        short type = header.readShort();
        // 读取ID
        int id = header.readInt();

        // 总长度-头部长度 = 数据部分长度
        int contentLength = length - HEADER_LENGTH;
        // ByteBuf中剩余字节数不够
        if(contentLength > data.readableBytes()){
            throw new RuntimeException("packet format error");
        }
        return builder().length(length)
                .type(type)
                .id(id)
                .data(data)
                .header(header)
                .build();
    }

    public byte[] getContent(){
        if(content == null){
            if(data.isDirect()){
                content = new byte[data.readableBytes()];
                data.readBytes(content);
                data.release();
            }else{
                content = data.array();
            }
        }
        return content;
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

    public void release(){
        data.release();
    }


}
