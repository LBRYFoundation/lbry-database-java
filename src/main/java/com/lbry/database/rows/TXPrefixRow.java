package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.TxKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.TxValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class TXPrefixRow extends PrefixRow<TxKey,TxValue>{

    public TXPrefixRow(PrefixDB database,RevertibleOperationStack operationStack){
        super(database,operationStack);
    }

    @Override
    public Prefix prefix(){
        return Prefix.TX;
    }

    @Override
    public byte[] packKey(TxKey key) {
        return ByteBuffer.allocate(1+32).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.tx_hash).array();
    }

    @Override
    public TxKey unpackKey(byte[] key){
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        TxKey keyObj = new TxKey();
        keyObj.tx_hash = new byte[32];
        bb.get(keyObj.tx_hash);
        return keyObj;
    }

    @Override
    public byte[] packValue(TxValue value) {
        return value.raw_tx;
    }

    @Override
    public TxValue unpackValue(byte[] value) {
        TxValue valueObj = new TxValue();
        valueObj.raw_tx = value;
        return valueObj;
    }

}
