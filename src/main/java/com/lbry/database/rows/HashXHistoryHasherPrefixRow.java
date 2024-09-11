package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.HashXHistoryHasherKey;
import com.lbry.database.values.HashXHistoryHasherValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HashXHistoryHasherPrefixRow extends PrefixRow<HashXHistoryHasherKey,HashXHistoryHasherValue>{

    public HashXHistoryHasherPrefixRow(PrefixDB database){
        super(database);
    }

    @Override
    public Prefix prefix(){
        return Prefix.HASHX_HISTORY_HASH;
    }

    @Override
    public byte[] packKey(HashXHistoryHasherKey key) {
        return ByteBuffer.allocate(1+11).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.hashX).array();
    }

    @Override
    public HashXHistoryHasherKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        HashXHistoryHasherKey keyObj = new HashXHistoryHasherKey();
        keyObj.hashX = new byte[11];
        bb.get(keyObj.hashX);
        return keyObj;
    }

    @Override
    public byte[] packValue(HashXHistoryHasherValue value) {
        //MessageDigest md;md.getProvider().
        //TODO SHA-256
        return new byte[0];
    }

    @Override
    public HashXHistoryHasherValue unpackValue(byte[] value) {
        //TODO SHA-256
        return null;
    }

}
