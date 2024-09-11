package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ChannelToClaimKey;
import com.lbry.database.values.ChannelToClaimValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ChannelToClaimPrefixRow extends PrefixRow<ChannelToClaimKey,ChannelToClaimValue>{

    public ChannelToClaimPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.CHANNEL_TO_CLAIM;
    }

    @Override
    public byte[] packKey(ChannelToClaimKey key) {
        byte[] strBytes = key.name.getBytes();
        return ByteBuffer.allocate(1+20+2+strBytes.length+4+2).order(ByteOrder.BIG_ENDIAN)
                .put(this.prefix().getValue())
                .put(key.signing_hash)
                .putShort((short) strBytes.length)
                .put(strBytes)
                .putInt(key.tx_num)
                .putShort(key.position)
                .array();
    }

    @Override
    public ChannelToClaimKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        ChannelToClaimKey keyObj = new ChannelToClaimKey();
        keyObj.signing_hash = new byte[20];
        bb.get(keyObj.signing_hash);
        byte[] strBytes = new byte[bb.getShort()];
        bb.get(strBytes);
        keyObj.name = new String(strBytes);
        keyObj.tx_num = bb.getInt();
        keyObj.position = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(ChannelToClaimValue value) {
        return ByteBuffer.allocate(20).order(ByteOrder.BIG_ENDIAN).put(value.claim_hash).array();
    }

    @Override
    public ChannelToClaimValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        ChannelToClaimValue valueObj = new ChannelToClaimValue();
        valueObj.claim_hash = new byte[20];
        bb.get(valueObj.claim_hash);
        return valueObj;
    }

}
