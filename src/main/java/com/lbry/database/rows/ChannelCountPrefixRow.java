package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ChannelCountKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.ChannelCountValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ChannelCountPrefixRow extends PrefixRow<ChannelCountKey,ChannelCountValue>{

    public ChannelCountPrefixRow(PrefixDB database,RevertibleOperationStack operationStack){
        super(database,operationStack);
    }

    @Override
    public Prefix prefix(){
        return Prefix.CHANNEL_COUNT;
    }

    @Override
    public byte[] packKey(ChannelCountKey key) {
        return ByteBuffer.allocate(1+20).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.channel_hash).array();
    }

    @Override
    public ChannelCountKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        ChannelCountKey keyObj = new ChannelCountKey();
        keyObj.channel_hash = new byte[20];
        bb.get(keyObj.channel_hash);
        return keyObj;
    }

    @Override
    public byte[] packValue(ChannelCountValue value) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value.count).array();
    }

    @Override
    public ChannelCountValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        ChannelCountValue valueObj = new ChannelCountValue();
        valueObj.count = bb.getInt();
        return valueObj;
    }

}
