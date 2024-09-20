package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.PendingActivationKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.PendingActivationValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class PendingActivationPrefixRow extends PrefixRow<PendingActivationKey,PendingActivationValue>{

    public PendingActivationPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.PENDING_ACTIVATION;
    }

    @Override
    public byte[] packKey(PendingActivationKey key) {
        return ByteBuffer.allocate(1+11).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putInt(key.height).put(key.txo_type).putInt(key.tx_num).putShort(key.position).array();
    }

    @Override
    public PendingActivationKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        PendingActivationKey keyObj = new PendingActivationKey();
        keyObj.height = bb.getInt();
        keyObj.txo_type = bb.get();
        keyObj.tx_num = bb.getInt();
        keyObj.position = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(PendingActivationValue value) {
        byte[] strBytes = value.normalized_name.getBytes();
        return ByteBuffer.allocate(20+2+strBytes.length).order(ByteOrder.BIG_ENDIAN).put(value.claim_hash).putShort((short) strBytes.length).put(strBytes).array();
    }

    @Override
    public PendingActivationValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        PendingActivationValue valueObj = new PendingActivationValue();
        valueObj.claim_hash = new byte[20];
        bb.get(valueObj.claim_hash);
        byte[] strBytes = new byte[bb.getShort()];
        bb.get(strBytes);
        valueObj.normalized_name = new String(strBytes);
        return valueObj;
    }

}
