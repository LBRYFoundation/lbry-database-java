package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ActiveAmountKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.ActiveAmountValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ActiveAmountPrefixRow extends PrefixRow<ActiveAmountKey,ActiveAmountValue>{

    public ActiveAmountPrefixRow(PrefixDB database,RevertibleOperationStack operationStack){
        super(database,operationStack);
    }

    @Override
    public Prefix prefix(){
        return Prefix.ACTIVE_AMOUNT;
    }

    @Override
    public byte[] packKey(ActiveAmountKey key) {
        return ByteBuffer.allocate(1+20+1+4+4+2).order(ByteOrder.BIG_ENDIAN)
                .put(this.prefix().getValue())
                .put(key.claim_hash)
                .put(key.txo_type)
                .putInt(key.activation_height)
                .putInt(key.tx_num)
                .putShort(key.position)
                .array();
    }

    @Override
    public ActiveAmountKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        ActiveAmountKey keyObj = new ActiveAmountKey();
        keyObj.claim_hash = new byte[20];
        bb.get(keyObj.claim_hash);
        keyObj.txo_type = bb.get();
        keyObj.activation_height = bb.getInt();
        keyObj.tx_num = bb.getInt();
        keyObj.position = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(ActiveAmountValue value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value.amount).array();
    }

    @Override
    public ActiveAmountValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        ActiveAmountValue valueObj = new ActiveAmountValue();
        valueObj.amount = bb.getLong();
        return valueObj;
    }

}
