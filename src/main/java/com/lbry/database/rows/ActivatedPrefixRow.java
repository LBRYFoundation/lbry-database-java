package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ActivationKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.ActivationValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class ActivatedPrefixRow extends PrefixRow<ActivationKey,ActivationValue>{

    public ActivatedPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.ACTIVATED_CLAIM_AND_SUPPORT;
    }

    @Override
    public byte[] packKey(ActivationKey key) {
        return ByteBuffer.allocate(1+1+4+2).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.txo_type).putInt(key.tx_num).putShort(key.position).array();
    }

    @Override
    public ActivationKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        ActivationKey keyObj = new ActivationKey();
        keyObj.txo_type = bb.get();
        keyObj.tx_num = bb.getInt();
        keyObj.position = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(ActivationValue value) {
        byte[] strBytes = value.normalized_name.getBytes();
        return ByteBuffer.allocate(4+20+2+strBytes.length).order(ByteOrder.BIG_ENDIAN).putInt(value.height).put(value.claim_hash).putShort((short) strBytes.length).put(strBytes).array();
    }

    @Override
    public ActivationValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        ActivationValue valueObj = new ActivationValue();
        valueObj.height = bb.getInt();
        valueObj.claim_hash = new byte[20];
        bb.get(valueObj.claim_hash);
        byte[] strBytes = new byte[bb.getShort()];
        bb.get(strBytes);
        valueObj.normalized_name = new String(strBytes);
        return valueObj;
    }

}
