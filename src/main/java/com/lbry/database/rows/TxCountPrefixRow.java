package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.TxCountKey;
import com.lbry.database.values.TxCountValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TxCountPrefixRow extends PrefixRow<TxCountKey, TxCountValue>{

    public TxCountPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.TX_COUNT;
    }

    @Override
    public byte[] packKey(TxCountKey key) {
        return ByteBuffer.allocate(1+4).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putInt(key.height).array();
    }

    @Override
    public TxCountKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        TxCountKey keyObj = new TxCountKey();
        keyObj.height = bb.getInt();
        return keyObj;
    }

    @Override
    public byte[] packValue(TxCountValue value) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value.tx_count).array();
    }

    @Override
    public TxCountValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        TxCountValue valueObj = new TxCountValue();
        valueObj.tx_count = bb.getInt();
        return valueObj;
    }

}