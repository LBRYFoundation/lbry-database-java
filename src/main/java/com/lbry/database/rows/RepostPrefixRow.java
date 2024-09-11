package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.RepostKey;
import com.lbry.database.values.RepostValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RepostPrefixRow extends PrefixRow<RepostKey,RepostValue>{

    public RepostPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.REPOST;
    }

    @Override
    public byte[] packKey(RepostKey key) {
        return ByteBuffer.allocate(1+20).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.claim_hash).array();
    }

    @Override
    public RepostKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        RepostKey keyObj = new RepostKey();
        keyObj.claim_hash = new byte[20];
        bb.get(keyObj.claim_hash);
        return null;
    }

    @Override
    public byte[] packValue(RepostValue value) {
        return value.reposted_claim_hash;
    }

    @Override
    public RepostValue unpackValue(byte[] value) {
        RepostValue valueObj = new RepostValue();
        valueObj.reposted_claim_hash = value;
        return valueObj;
    }

}
