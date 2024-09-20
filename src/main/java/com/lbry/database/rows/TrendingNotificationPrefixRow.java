package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.TrendingNotificationKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.TrendingNotificationValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class TrendingNotificationPrefixRow extends PrefixRow<TrendingNotificationKey,TrendingNotificationValue>{

    public TrendingNotificationPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.TRENDING_NOTIFICATION;
    }

    @Override
    public byte[] packKey(TrendingNotificationKey key) {
        return ByteBuffer.allocate(1+4+20).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putInt(key.height).put(key.claim_hash).array();
    }

    @Override
    public TrendingNotificationKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        TrendingNotificationKey keyObj = new TrendingNotificationKey();
        keyObj.height = bb.getInt();
        keyObj.claim_hash = new byte[20];
        bb.get(keyObj.claim_hash);
        return keyObj;
    }

    @Override
    public byte[] packValue(TrendingNotificationValue value) {
        return ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN).putLong(value.previous_amount).putLong(value.new_amount).array();
    }

    @Override
    public TrendingNotificationValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        TrendingNotificationValue valueObj = new TrendingNotificationValue();
        valueObj.previous_amount = bb.getLong();
        valueObj.new_amount = bb.getLong();
        return valueObj;
    }

}
