package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.MempoolTxKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.MempoolTxValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class MempoolTXPrefixRow extends PrefixRow<MempoolTxKey,MempoolTxValue>{

    public MempoolTXPrefixRow(PrefixDB database,RevertibleOperationStack operationStack){
        super(database,operationStack);
    }

    @Override
    public Prefix prefix(){
        return Prefix.MEMPOOL_TX;
    }

    @Override
    public byte[] packKey(MempoolTxKey key) {
        return ByteBuffer.allocate(1+32).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.tx_hash).array();
    }

    @Override
    public MempoolTxKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        MempoolTxKey keyObj = new MempoolTxKey();
        keyObj.tx_hash = new byte[32];
        bb.get(keyObj.tx_hash);
        return keyObj;
    }

    @Override
    public byte[] packValue(MempoolTxValue value) {
        return value.raw_tx;
    }

    @Override
    public MempoolTxValue unpackValue(byte[] value) {
        MempoolTxValue valueObj = new MempoolTxValue();
        valueObj.raw_tx = value;
        return valueObj;
    }

}
