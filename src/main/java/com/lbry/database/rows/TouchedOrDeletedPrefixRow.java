package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.TouchedOrDeletedClaimKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.TouchedOrDeletedClaimValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashSet;

public class TouchedOrDeletedPrefixRow extends PrefixRow<TouchedOrDeletedClaimKey,TouchedOrDeletedClaimValue>{

    public TouchedOrDeletedPrefixRow(PrefixDB database,RevertibleOperationStack operationStack){
        super(database,operationStack);
    }

    @Override
    public Prefix prefix(){
        return Prefix.TOUCHED_OR_DELETED;
    }

    @Override
    public byte[] packKey(TouchedOrDeletedClaimKey key) {
        return ByteBuffer.allocate(1+4).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putInt(key.height).array();
    }

    @Override
    public TouchedOrDeletedClaimKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        TouchedOrDeletedClaimKey keyValue = new TouchedOrDeletedClaimKey();
        keyValue.height = bb.getInt();
        return keyValue;
    }

    @Override
    public byte[] packValue(TouchedOrDeletedClaimValue value) {
        ByteBuffer bb = ByteBuffer.allocate(8+value.touched_claims.size()*20+value.deleted_claims.size()*20).order(ByteOrder.BIG_ENDIAN);
        bb.putInt(value.touched_claims.size());
        bb.putInt(value.deleted_claims.size());
        for(byte[] touched : value.touched_claims){
            assert touched.length==20 : "Every touched item should have a length of 20 bytes.";
            bb.put(touched);
        }
        for(byte[] deleted : value.deleted_claims){
            assert deleted.length==20 : "Every deleted item should have a length of 20 bytes.";
            bb.put(deleted);
        }
        return bb.array();
    }

    @Override
    public TouchedOrDeletedClaimValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        int touchedAmount = bb.getInt();
        int deletedAmount = bb.getInt();
        assert value.length==8+touchedAmount+deletedAmount : "Data has too less or too much bytes.";
        TouchedOrDeletedClaimValue valueObj = new TouchedOrDeletedClaimValue();
        valueObj.touched_claims = new LinkedHashSet<>();
        for(int i=0;i<touchedAmount;i++){
            byte[] touchedClaim = new byte[20];
            bb.get(touchedClaim);
            valueObj.touched_claims.add(touchedClaim);
        }
        valueObj.deleted_claims = new LinkedHashSet<>();
        for(int i=0;i<deletedAmount;i++){
            byte[] deletedClaim = new byte[20];
            bb.get(deletedClaim);
            valueObj.deleted_claims.add(deletedClaim);
        }
        return valueObj;
    }

}
