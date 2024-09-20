package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.TXOToClaimKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.TXOToClaimValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class TXOToClaimPrefixRow extends PrefixRow<TXOToClaimKey, TXOToClaimValue>{

    public TXOToClaimPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.TXO_TO_CLAIM;
    }

    @Override
    public byte[] packKey(TXOToClaimKey key) {
        return ByteBuffer.allocate(1+6).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putInt(key.tx_num).putShort(key.position).array();
    }

    @Override
    public TXOToClaimKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        TXOToClaimKey keyObj = new TXOToClaimKey();
        keyObj.tx_num = bb.getInt();
        keyObj.position = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(TXOToClaimValue value) {
        byte[] strBytes = value.name.getBytes();
        return ByteBuffer.allocate(20+2+strBytes.length).order(ByteOrder.BIG_ENDIAN).put(value.claim_hash).putShort((short) strBytes.length).put(strBytes).array();
    }

    @Override
    public TXOToClaimValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        TXOToClaimValue valueObj = new TXOToClaimValue();
        valueObj.claim_hash = new byte[20];
        bb.get(valueObj.claim_hash);
        byte[] strBytes = new byte[bb.getShort()];
        bb.get(strBytes);
        valueObj.name = new String(strBytes);
        return valueObj;
    }

}
