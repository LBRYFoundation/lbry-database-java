package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.TxNumKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.TxNumValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class TXNumPrefixRow extends PrefixRow<TxNumKey,TxNumValue>{

    public TXNumPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.TX_NUM;
    }

    @Override
    public byte[] packKey(TxNumKey key) {
        return ByteBuffer.allocate(1+32).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.tx_hash).array();
    }

    @Override
    public TxNumKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        TxNumKey keyObj = new TxNumKey();
        keyObj.tx_hash = new byte[32];
        bb.get(keyObj.tx_hash);
        return keyObj;
    }

    @Override
    public byte[] packValue(TxNumValue value) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value.tx_num).array();
    }

    @Override
    public TxNumValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        TxNumValue valueObj = new TxNumValue();
        valueObj.tx_num = bb.getInt();
        return valueObj;
    }

}
