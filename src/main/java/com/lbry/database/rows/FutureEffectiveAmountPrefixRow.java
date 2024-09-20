package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.FutureEffectiveAmountKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.FutureEffectiveAmountValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class FutureEffectiveAmountPrefixRow extends PrefixRow<FutureEffectiveAmountKey,FutureEffectiveAmountValue>{

    public FutureEffectiveAmountPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.FUTURE_EFFECTIVE_AMOUNT;
    }

    @Override
    public byte[] packKey(FutureEffectiveAmountKey key) {
        return ByteBuffer.allocate(1+20).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.claim_hash).array();
    }

    @Override
    public FutureEffectiveAmountKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        FutureEffectiveAmountKey keyObj = new FutureEffectiveAmountKey();
        keyObj.claim_hash = new byte[20];
        bb.get(keyObj.claim_hash);
        return keyObj;
    }

    @Override
    public byte[] packValue(FutureEffectiveAmountValue value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value.future_effective_amount).array();
    }

    @Override
    public FutureEffectiveAmountValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        FutureEffectiveAmountValue valueObj = new FutureEffectiveAmountValue();
        valueObj.future_effective_amount = bb.getLong();
        return valueObj;
    }

}
