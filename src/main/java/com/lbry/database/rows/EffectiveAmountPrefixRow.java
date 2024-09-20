package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.EffectiveAmountKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.EffectiveAmountValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class EffectiveAmountPrefixRow extends PrefixRow<EffectiveAmountKey,EffectiveAmountValue>{

    public EffectiveAmountPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.EFFECTIVE_AMOUNT;
    }

    @Override
    public byte[] packKey(EffectiveAmountKey key) {
        return ByteBuffer.allocate(1+20).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.claim_hash).array();
    }

    @Override
    public EffectiveAmountKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        EffectiveAmountKey keyObj = new EffectiveAmountKey();
        keyObj.claim_hash = new byte[20];
        bb.get(keyObj.claim_hash);
        return keyObj;
    }

    @Override
    public byte[] packValue(EffectiveAmountValue value) {
        assert value.activated_sum >= value.activated_support_sum : "Effective amount should be larger than support sum.";
        return ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN).putLong(value.activated_sum).putLong(value.activated_support_sum).array();
    }

    @Override
    public EffectiveAmountValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        EffectiveAmountValue valueObj = new EffectiveAmountValue();
        valueObj.activated_sum = bb.getLong();
        valueObj.activated_support_sum = bb.getLong();
        return valueObj;
    }

}
