package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.HashXUTXOKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.HashXUTXOValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HashXUTXOPrefixRow extends PrefixRow<HashXUTXOKey,HashXUTXOValue>{

    public HashXUTXOPrefixRow(PrefixDB database,RevertibleOperationStack operationStack){
        super(database,operationStack);
    }

    @Override
    public Prefix prefix(){
        return Prefix.HASHX_UTXO;
    }

    @Override
    public byte[] packKey(HashXUTXOKey key) {
        return ByteBuffer.allocate(1+4+4+2).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.short_tx_hash).putInt(key.tx_num).putShort(key.nout).array();
    }

    @Override
    public HashXUTXOKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        HashXUTXOKey keyObj = new HashXUTXOKey();
        keyObj.short_tx_hash = new byte[4];
        bb.get(keyObj.short_tx_hash);
        keyObj.tx_num = bb.getInt();
        keyObj.nout = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(HashXUTXOValue value) {
        return ByteBuffer.allocate(11).order(ByteOrder.BIG_ENDIAN).put(value.hashX).array();
    }

    @Override
    public HashXUTXOValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        HashXUTXOValue valueObj = new HashXUTXOValue();
        valueObj.hashX = new byte[11];
        bb.get(valueObj.hashX);
        return valueObj;
    }

}
