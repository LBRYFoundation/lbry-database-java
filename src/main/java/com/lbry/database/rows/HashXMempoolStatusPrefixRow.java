package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.HashXStatusKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.HashXStatusValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class HashXMempoolStatusPrefixRow extends PrefixRow<HashXStatusKey,HashXStatusValue>{

    public HashXMempoolStatusPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.HASHX_MEMPOOL_STATUS;
    }

    @Override
    public byte[] packKey(HashXStatusKey key) {
        return ByteBuffer.allocate(1+20).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.hashX).array();
    }

    @Override
    public HashXStatusKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        HashXStatusKey keyObj = new HashXStatusKey();
        keyObj.hashX = new byte[20];
        bb.get(keyObj.hashX);
        return keyObj;
    }

    @Override
    public byte[] packValue(HashXStatusValue value) {
        return ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN).put(value.status).array();
    }

    @Override
    public HashXStatusValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        HashXStatusValue keyObj = new HashXStatusValue();
        keyObj.status = new byte[32];
        bb.get(keyObj.status);
        return keyObj;
    }

}
