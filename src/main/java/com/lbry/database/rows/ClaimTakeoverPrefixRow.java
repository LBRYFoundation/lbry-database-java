package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ClaimTakeoverKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.ClaimTakeoverValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ClaimTakeoverPrefixRow extends PrefixRow<ClaimTakeoverKey,ClaimTakeoverValue>{

    public ClaimTakeoverPrefixRow(PrefixDB database,RevertibleOperationStack operationStack){
        super(database,operationStack);
    }

    @Override
    public Prefix prefix(){
        return Prefix.CLAIM_TAKEOVER;
    }

    @Override
    public byte[] packKey(ClaimTakeoverKey key) {
        byte[] strBytes = key.normalized_name.getBytes();
        return ByteBuffer.allocate(1+2+strBytes.length).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putShort((short) strBytes.length).put(strBytes).array();
    }

    @Override
    public ClaimTakeoverKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        ClaimTakeoverKey keyObj = new ClaimTakeoverKey();
        byte[] strBytes = new byte[bb.getShort()];
        bb.get(strBytes);
        keyObj.normalized_name = new String(strBytes);
        return keyObj;
    }

    @Override
    public byte[] packValue(ClaimTakeoverValue value) {
        return ByteBuffer.allocate(20+4).order(ByteOrder.BIG_ENDIAN).put(value.claim_hash).putInt(value.height).array();
    }

    @Override
    public ClaimTakeoverValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        ClaimTakeoverValue valueObj = new ClaimTakeoverValue();
        valueObj.claim_hash = new byte[20];
        bb.get(valueObj.claim_hash);
        valueObj.height = bb.getInt();
        return valueObj;
    }

}
