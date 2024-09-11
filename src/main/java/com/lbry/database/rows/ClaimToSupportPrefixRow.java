package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ClaimToSupportKey;
import com.lbry.database.values.ClaimToSupportValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ClaimToSupportPrefixRow extends PrefixRow<ClaimToSupportKey,ClaimToSupportValue>{

    public ClaimToSupportPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.CLAIM_TO_SUPPORT;
    }

    @Override
    public byte[] packKey(ClaimToSupportKey key) {
        return ByteBuffer.allocate(1+20+4+2).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.claim_hash).putInt(key.tx_hash).putShort(key.position).array();
    }

    @Override
    public ClaimToSupportKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        ClaimToSupportKey keyObj = new ClaimToSupportKey();
        keyObj.claim_hash = new byte[20];
        bb.get(keyObj.claim_hash);
        keyObj.tx_hash = bb.getInt();
        keyObj.position = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(ClaimToSupportValue value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value.amount).array();
    }

    @Override
    public ClaimToSupportValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        ClaimToSupportValue valueObj = new ClaimToSupportValue();
        valueObj.amount = bb.getLong();
        return valueObj;
    }

}