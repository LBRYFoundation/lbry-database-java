package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.BidOrderKey;
import com.lbry.database.values.BidOrderValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BidOrderPrefixRow extends PrefixRow<BidOrderKey,BidOrderValue>{

    public BidOrderPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.BID_ORDER;
    }

    @Override
    public byte[] packKey(BidOrderKey key) {
        byte[] strBytes = key.normalized_name.getBytes();
        return ByteBuffer.allocate(1+2+strBytes.length+8+4+2).order(ByteOrder.BIG_ENDIAN)
                .put(this.prefix().getValue())
                .putShort((short) strBytes.length)
                .put(strBytes)
                .putLong(0xFFFFFFFFFFFFFFFFL - key.effective_amount)
                .putInt(key.tx_num)
                .putShort(key.position)
                .array();
    }

    @Override
    public BidOrderKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        BidOrderKey keyObj = new BidOrderKey();
        byte[] strBytes = new byte[bb.getShort()];
        bb.get(strBytes);
        keyObj.normalized_name = new String(strBytes);
        keyObj.effective_amount = 0xFFFFFFFFFFFFFFFFL - bb.getLong();
        keyObj.tx_num = bb.getInt();
        keyObj.position = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(BidOrderValue value) {
        return ByteBuffer.allocate(20).order(ByteOrder.BIG_ENDIAN).put(value.claim_hash).array();
    }

    @Override
    public BidOrderValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        BidOrderValue valueObj = new BidOrderValue();
        valueObj.claim_hash = new byte[20];
        bb.get(valueObj.claim_hash);
        return valueObj;
    }

}
