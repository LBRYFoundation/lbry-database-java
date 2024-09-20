package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.RepostedKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.RepostedValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class RepostedPrefixRow extends PrefixRow<RepostedKey,RepostedValue>{

    public RepostedPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.REPOSTED_CLAIM;
    }

    @Override
    public byte[] packKey(RepostedKey key) {
        return ByteBuffer.allocate(1+20+4+2).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.reposted_claim_hash).putInt(key.tx_num).putShort(key.position).array();
    }

    @Override
    public RepostedKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        RepostedKey keyObj = new RepostedKey();
        keyObj.reposted_claim_hash = new byte[20];
        bb.get(keyObj.reposted_claim_hash);
        keyObj.tx_num = bb.getInt();
        keyObj.position = bb.getShort();
        return keyObj;
    }

    @Override
    public byte[] packValue(RepostedValue value) {
        return ByteBuffer.allocate(20).order(ByteOrder.BIG_ENDIAN).put(value.claim_hash).array();
    }

    @Override
    public RepostedValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        RepostedValue valueObj = new RepostedValue();
        valueObj.claim_hash = new byte[20];
        bb.get(valueObj.claim_hash);
        return valueObj;
    }

}
