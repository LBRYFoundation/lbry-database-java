package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.TxHashKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.TxHashValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class TXHashPrefixRow extends PrefixRow<TxHashKey, TxHashValue>{

    public TXHashPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.TX_HASH;
    }

    @Override
    public byte[] packKey(TxHashKey key) {
        return ByteBuffer.allocate(1+4).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putInt(key.tx_num).array();
    }

    @Override
    public TxHashKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        TxHashKey keyObj = new TxHashKey();
        keyObj.tx_num = bb.getInt();
        return keyObj;
    }

    @Override
    public byte[] packValue(TxHashValue value) {
        return ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN).put(value.tx_hash).array();
    }

    @Override
    public TxHashValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        TxHashValue valueObj = new TxHashValue();
        valueObj.tx_hash = new byte[32];
        bb.get(valueObj.tx_hash);
        return valueObj;
    }

}
