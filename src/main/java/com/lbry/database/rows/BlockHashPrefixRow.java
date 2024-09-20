package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.BlockHashKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.BlockHashValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BlockHashPrefixRow extends PrefixRow<BlockHashKey,BlockHashValue>{

    public BlockHashPrefixRow(PrefixDB database,RevertibleOperationStack operationStack){
        super(database,operationStack);
    }

    @Override
    public Prefix prefix(){
        return Prefix.BLOCK_HASH;
    }

    @Override
    public byte[] packKey(BlockHashKey key) {
        return ByteBuffer.allocate(1+4).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putInt(key.height).array();
    }

    @Override
    public BlockHashKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        BlockHashKey keyObj = new BlockHashKey();
        keyObj.height = bb.getInt();
        return keyObj;
    }

    @Override
    public byte[] packValue(BlockHashValue value) {
        return ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN).put(value.block_hash).array();
    }

    @Override
    public BlockHashValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        BlockHashValue valueObj = new BlockHashValue();
        valueObj.block_hash = new byte[32];
        bb.get(valueObj.block_hash);
        return valueObj;
    }

}
