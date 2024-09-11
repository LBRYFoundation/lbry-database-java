package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.HashXHistoryKey;
import com.lbry.database.values.HashXHistoryValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;

public class HashXHistoryPrefixRow extends PrefixRow<HashXHistoryKey,HashXHistoryValue>{

    public HashXHistoryPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.HASHX_HISTORY;
    }

    @Override
    public byte[] packKey(HashXHistoryKey key) {
        return ByteBuffer.allocate(1+11+4).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.hashX).putInt(key.height).array();
    }

    @Override
    public HashXHistoryKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        HashXHistoryKey keyObj = new HashXHistoryKey();
        keyObj.hashX = new byte[11];
        bb.get(keyObj.hashX);
        keyObj.height = bb.getInt();
        return keyObj;
    }

    @Override
    public byte[] packValue(HashXHistoryValue value) {
        ByteBuffer bb = ByteBuffer.allocate(value.tx_nums.size()*4).order(ByteOrder.BIG_ENDIAN);
        for(int txNum : value.tx_nums){
            bb.putInt(txNum);
        }
        return bb.array();
    }

    @Override
    public HashXHistoryValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        HashXHistoryValue valueObj = new HashXHistoryValue();
        valueObj.tx_nums = new ArrayList<>();
        for(int i=0;i<value.length/4;i++){
            valueObj.tx_nums.add(bb.getInt());
        }
        return valueObj;
    }

}
