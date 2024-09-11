package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.BlockTxsKey;
import com.lbry.database.values.BlockTxsValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

public class BlockTxsPrefixRow extends PrefixRow<BlockTxsKey,BlockTxsValue>{

    public BlockTxsPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.BLOCK_TX;
    }

    @Override
    public byte[] packKey(BlockTxsKey key) {
        return ByteBuffer.allocate(1+4).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putInt(key.height).array();
    }

    @Override
    public BlockTxsKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        BlockTxsKey keyObj = new BlockTxsKey();
        keyObj.height = bb.getInt();
        return keyObj;
    }

    @Override
    public byte[] packValue(BlockTxsValue value) {
        ByteBuffer bb = ByteBuffer.allocate(value.tx_hashes.size()*32).order(ByteOrder.BIG_ENDIAN);
        for(byte[] txHash : value.tx_hashes){
            bb.put(txHash);
        }
        return bb.array();
    }

    @Override
    public BlockTxsValue unpackValue(byte[] value){
        BlockTxsValue valueObj = new BlockTxsValue();
        valueObj.tx_hashes = new ArrayList<>();
        for(int i=0;i<value.length/32;i++){
            byte[] txHash = new byte[32];
            System.arraycopy(value,i*32,txHash,0,txHash.length);
            valueObj.tx_hashes.add(txHash);
        }
        return valueObj;
    }

}
