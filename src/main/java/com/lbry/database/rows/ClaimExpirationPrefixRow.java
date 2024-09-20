package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ClaimExpirationKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.ClaimExpirationValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ClaimExpirationPrefixRow extends PrefixRow<ClaimExpirationKey,ClaimExpirationValue>{

    public ClaimExpirationPrefixRow(PrefixDB database,RevertibleOperationStack operationStack){
        super(database,operationStack);
    }

    @Override
    public Prefix prefix(){
        return Prefix.CLAIM_EXPIRATION;
    }

    @Override
    public byte[] packKey(ClaimExpirationKey key) {
        return ByteBuffer.allocate(1+4+4+2).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putInt(key.expiration).putInt(key.tx_num).putShort(key.position).array();
    }

    @Override
    public ClaimExpirationKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        ClaimExpirationKey keyObj = new ClaimExpirationKey();
        keyObj.expiration = bb.getInt();
        keyObj.tx_num = bb.getInt();
        keyObj.position = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(ClaimExpirationValue value) {
        byte[] strBytes = value.normalized_name.getBytes();
        return ByteBuffer.allocate(20+2+strBytes.length).order(ByteOrder.BIG_ENDIAN).put(value.claim_hash).putShort((short) strBytes.length).put(strBytes).array();
    }

    @Override
    public ClaimExpirationValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        ClaimExpirationValue valueObj = new ClaimExpirationValue();
        valueObj.claim_hash = new byte[20];
        bb.get(valueObj.claim_hash);
        byte[] strBytes = new byte[bb.getShort()];
        bb.get(strBytes);
        valueObj.normalized_name = new String(strBytes);
        return valueObj;
    }

}
