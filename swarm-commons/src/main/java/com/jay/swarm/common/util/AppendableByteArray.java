package com.jay.swarm.common.util;

/**
 * <p>
 *
 * </p>
 *
 * @author Jay
 * @date 2021/12/09 20:51
 */
public class AppendableByteArray {
    private byte[] array = new byte[128];
    private int size = 0;

    public void append(byte b){
        if(array.length == size){
            byte[] old = array;
            array = new byte[old.length << 1];
            System.arraycopy(old, 0, array, 0, old.length);
        }
        array[size++] = b;
    }

    public byte[] array(){
        byte[] res = new byte[size];
        System.arraycopy(array, 0, res, 0, size);
        return res;
    }

    public int size(){
        return size;
    }

    public void flush(){
        size = 0;
        array = new byte[128];
    }
}
