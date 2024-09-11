package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.RepostedCountKey;
import com.lbry.database.values.RepostedCountValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RepostedCountPrefixRow extends PrefixRow<RepostedCountKey,RepostedCountValue>{

    public RepostedCountPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.REPOSTED_COUNT;
    }

    @Override
    public byte[] packKey(RepostedCountKey key) {
        return ByteBuffer.allocate(1+20).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.claim_hash).array();
    }

    @Override
    public RepostedCountKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        RepostedCountKey keyObj = new RepostedCountKey();
        keyObj.claim_hash = new byte[20];
        bb.get(keyObj.claim_hash);
        return keyObj;
    }

    @Override
    public byte[] packValue(RepostedCountValue value) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value.reposted_count).array();
    }

    @Override
    public RepostedCountValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        RepostedCountValue valueObj = new RepostedCountValue();
        valueObj.reposted_count = bb.getInt();
        return valueObj;
    }

}
