package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.SupportAmountKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.SupportAmountValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SupportAmountPrefixRow extends PrefixRow<SupportAmountKey,SupportAmountValue>{

    public SupportAmountPrefixRow(PrefixDB database,RevertibleOperationStack operationStack){
        super(database,operationStack);
    }

    @Override
    public Prefix prefix(){
        return Prefix.SUPPORT_AMOUNT;
    }

    @Override
    public byte[] packKey(SupportAmountKey key) {
        return ByteBuffer.allocate(1+20).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.claim_hash).array();
    }

    @Override
    public SupportAmountKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        SupportAmountKey keyObj = new SupportAmountKey();
        keyObj.claim_hash = new byte[20];
        bb.get(keyObj.claim_hash);
        return keyObj;
    }

    @Override
    public byte[] packValue(SupportAmountValue value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value.amount).array();
    }

    @Override
    public SupportAmountValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        SupportAmountValue valueObj = new SupportAmountValue();
        valueObj.amount = bb.getLong();
        return valueObj;
    }

}
