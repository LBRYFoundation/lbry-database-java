package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.BlockHeaderKey;
import com.lbry.database.values.BlockHeaderValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BlockHeaderPrefixRow extends PrefixRow<BlockHeaderKey,BlockHeaderValue>{

    public BlockHeaderPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.HEADER;
    }

    @Override
    public byte[] packKey(BlockHeaderKey key) {
        return ByteBuffer.allocate(1+4).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putInt(key.height).array();
    }

    @Override
    public BlockHeaderKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        BlockHeaderKey keyObj = new BlockHeaderKey();
        keyObj.height = bb.getInt();
        return keyObj;
    }

    @Override
    public byte[] packValue(BlockHeaderValue value) {
        return ByteBuffer.allocate(112).order(ByteOrder.BIG_ENDIAN).put(value.header).array();
    }

    @Override
    public BlockHeaderValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value);
        BlockHeaderValue valueObj = new BlockHeaderValue();
        valueObj.header = new byte[112];
        bb.get(valueObj.header);
        return valueObj;
    }

}
