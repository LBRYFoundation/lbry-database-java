package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ClaimToChannelKey;
import com.lbry.database.values.ClaimToChannelValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ClaimToChannelPrefixRow extends PrefixRow<ClaimToChannelKey,ClaimToChannelValue>{

    public ClaimToChannelPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.CLAIM_TO_CHANNEL;
    }

    @Override
    public byte[] packKey(ClaimToChannelKey key) {
        return ByteBuffer.allocate(1+20+4+2).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.claim_hash).putInt(key.tx_num).putShort(key.position).array();
    }

    @Override
    public ClaimToChannelKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        ClaimToChannelKey keyObj = new ClaimToChannelKey();
        keyObj.claim_hash = new byte[20];
        bb.get(keyObj.claim_hash);
        keyObj.tx_num = bb.getInt();
        keyObj.position = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(ClaimToChannelValue value) {
        return ByteBuffer.allocate(20).order(ByteOrder.BIG_ENDIAN).put(value.signing_hash).array();
    }

    @Override
    public ClaimToChannelValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        ClaimToChannelValue valueObj = new ClaimToChannelValue();
        valueObj.signing_hash = new byte[20];
        bb.get(valueObj.signing_hash);
        return valueObj;
    }

}
