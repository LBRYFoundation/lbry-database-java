package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ClaimShortIDKey;
import com.lbry.database.values.ClaimShortIDValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ClaimShortIDPrefixRow extends PrefixRow<ClaimShortIDKey,ClaimShortIDValue>{

    public ClaimShortIDPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.CLAIM_SHORT_ID_PREFIX;
    }

    @Override
    public byte[] packKey(ClaimShortIDKey key) {
        byte[] strBytesName = key.normalized_name.getBytes();
        byte[] strBytesClaimID = key.partial_claim_id.getBytes();
        return ByteBuffer.allocate(1+2+strBytesName.length+2+strBytesClaimID.length+4+2).order(ByteOrder.BIG_ENDIAN)
                .put(this.prefix().getValue())
                .putShort((short) strBytesName.length)
                .put(strBytesName)
                .putShort((short) strBytesClaimID.length)
                .put(strBytesClaimID)
                .putInt(key.root_tx_num)
                .putShort(key.root_position)
                .array();
    }

    @Override
    public ClaimShortIDKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        ClaimShortIDKey keyObj = new ClaimShortIDKey();
        byte[] strBytesName = new byte[bb.getShort()];
        bb.get(strBytesName);
        keyObj.normalized_name = new String(strBytesName);
        byte[] strBytesClaimID = new byte[bb.getShort()];
        bb.get(strBytesClaimID);
        keyObj.partial_claim_id = new String(strBytesClaimID);
        keyObj.root_tx_num = bb.getInt();
        keyObj.root_position = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(ClaimShortIDValue value) {
        return ByteBuffer.allocate(4+2).order(ByteOrder.BIG_ENDIAN).putInt(value.tx_num).putShort(value.position).array();
    }

    @Override
    public ClaimShortIDValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        ClaimShortIDValue valueObj = new ClaimShortIDValue();
        valueObj.tx_num = bb.getInt();
        valueObj.position = bb.getShort();
        return valueObj;
    }

}
