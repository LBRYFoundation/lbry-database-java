package com.lbry.database.revert;

import com.lbry.database.Prefix;
import com.lbry.database.rows.PrefixRow;
import com.lbry.database.util.Tuple2;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public abstract class RevertibleOperation{

    protected byte[] key;
    protected byte[] value;

    protected boolean isPut;

    public RevertibleOperation(byte[] key,byte[] value){
        this.key = key;
        this.value = value;
    }

    public byte[] getKey(){
        return this.key;
    }

    public byte[] getValue(){
        return this.value;
    }

    public boolean isPut(){
        return this.isPut;
    }

    public boolean isDelete(){
        return !this.isPut;
    }

    public RevertibleOperation invert(){
        throw new RuntimeException("Not implemented");
    }

    public byte[] pack(){
        return ByteBuffer.allocate(1+4+4+this.key.length+this.value.length).order(ByteOrder.BIG_ENDIAN)
                .put((byte) (this.isPut?0x01:0x00))
                .putInt(this.key.length)
                .putInt(this.value.length)
                .put(this.key)
                .put(this.value)
                .array();
    }

    public static Tuple2<RevertibleOperation,byte[]> unpack(byte[] packed){
        ByteBuffer bb = ByteBuffer.wrap(packed).order(ByteOrder.BIG_ENDIAN);
        boolean isPut = (bb.get() & 0xFF)!=0x00;
        int keyLength = bb.getInt();
        int valueLength = bb.getInt();
        byte[] keyBytes = new byte[keyLength];
        bb.get(keyBytes);
        byte[] valueBytes = new byte[valueLength];
        bb.get(valueBytes);
        byte[] remainingPacked = new byte[bb.remaining()];
        bb.get(remainingPacked);
        if(isPut){
            return new Tuple2<>(new RevertiblePut(keyBytes,valueBytes),remainingPacked);
        }
        return new Tuple2<>(new RevertibleDelete(keyBytes,valueBytes),remainingPacked);
    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof RevertibleOperation){
            RevertibleOperation op = (RevertibleOperation) obj;
            return this.isPut==op.isPut && Arrays.equals(this.key,op.key) && Arrays.equals(this.value,op.value);
        }
        return false;
    }

    @Override
    public String toString() {
        Prefix prefix = Prefix.getByValue(this.key[0]);
        String prefixStr = (prefix!=null?prefix.name():"?");
        String k = "?";
        String v = "?";
        if(prefix!=null){
            k = PrefixRow.TYPES.get(prefix).unpackKey(this.key).toString();
            v = PrefixRow.TYPES.get(prefix).unpackValue(this.value).toString();
        }
        return (this.isPut?"PUT":"DELETE")+" "+prefixStr+": "+k+" | "+v;
    }

}