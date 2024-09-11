package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.SupportToClaimKey;
import com.lbry.database.values.SupportToClaimValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SupportToClaimPrefixRow extends PrefixRow<SupportToClaimKey,SupportToClaimValue>{

    public SupportToClaimPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.SUPPORT_TO_CLAIM;
    }

    @Override
    public byte[] packKey(SupportToClaimKey key) {
        return ByteBuffer.allocate(7).order(ByteOrder.BIG_ENDIAN).put(Prefix.SUPPORT_TO_CLAIM.getValue()).putInt(key.tx_num).putShort(key.position).array();
    }

    @Override
    public byte[] packValue(SupportToClaimValue value) {
        return ByteBuffer.allocate(1+20).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(value.claim_hash).array();
    }

    @Override
    public SupportToClaimKey unpackKey(byte[] key){
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        SupportToClaimKey keyObj = new SupportToClaimKey();
        keyObj.tx_num = bb.getInt();
        keyObj.position = bb.getShort();
        return keyObj;
    }

    @Override
    public SupportToClaimValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        SupportToClaimValue valueObj = new SupportToClaimValue();
        valueObj.claim_hash = new byte[20];
        bb.get(valueObj.claim_hash);
        return valueObj;
    }

}
