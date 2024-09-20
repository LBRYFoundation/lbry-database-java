package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.UndoKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.UndoValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class UndoPrefixRow extends PrefixRow<UndoKey,UndoValue>{

    public UndoPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.UNDO;
    }

    @Override
    public byte[] packKey(UndoKey key) {
        return ByteBuffer.allocate(1+8+32).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putLong(key.height).put(key.block_hash).array();
    }

    @Override
    public UndoKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        UndoKey keyObj = new UndoKey();
        keyObj.height = bb.getLong();
        keyObj.block_hash = new byte[32];
        bb.get(keyObj.block_hash);
        return keyObj;
    }

    @Override
    public byte[] packValue(UndoValue value) {
        return value.data;
    }

    @Override
    public UndoValue unpackValue(byte[] value) {
        UndoValue valueObj = new UndoValue();
        valueObj.data = value;
        return valueObj;
    }

}
