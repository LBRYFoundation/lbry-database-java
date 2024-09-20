package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.UTXOKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.UTXOValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class UTXOPrefixRow extends PrefixRow<UTXOKey,UTXOValue>{

    public UTXOPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.UTXO;
    }

    @Override
    public byte[] packKey(UTXOKey key) {
        return ByteBuffer.allocate(1+11+4+2).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.hashX).putInt(key.tx_num).putShort(key.nout).array();
    }

    @Override
    public UTXOKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        UTXOKey keyObj = new UTXOKey();
        keyObj.hashX = new byte[11];
        bb.get(keyObj.hashX);
        keyObj.tx_num = bb.getInt();
        keyObj.nout = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(UTXOValue value) {
        return ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(value.amount).array();
    }

    @Override
    public UTXOValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        UTXOValue valueObj = new UTXOValue();
        valueObj.amount = bb.getLong();
        return valueObj;
    }

}
