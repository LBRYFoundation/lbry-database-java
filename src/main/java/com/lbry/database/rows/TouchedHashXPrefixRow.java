package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.TouchedHashXKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.TouchedHashXValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class TouchedHashXPrefixRow extends PrefixRow<TouchedHashXKey,TouchedHashXValue>{

    public TouchedHashXPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.TOUCHED_HASHX;
    }

    @Override
    public byte[] packKey(TouchedHashXKey key) {
        return ByteBuffer.allocate(1+4).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).putInt(key.height).array();
    }

    @Override
    public TouchedHashXKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        TouchedHashXKey keyObj = new TouchedHashXKey();
        keyObj.height = bb.getInt();
        return keyObj;
    }

    @Override
    public byte[] packValue(TouchedHashXValue value) {
        ByteBuffer bb = ByteBuffer.allocate(value.touched_hashXs.size()*11).order(ByteOrder.BIG_ENDIAN);
        for(byte[] hashX : value.touched_hashXs){
            bb.put(hashX);
        }
        return bb.array();
    }

    @Override
    public TouchedHashXValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        TouchedHashXValue valueObj = new TouchedHashXValue();
        valueObj.touched_hashXs = new ArrayList<>();
        for(int i=0;i<value.length/11;i++){
            byte[] hashX = new byte[11];
            bb.get(hashX);
            valueObj.touched_hashXs.add(hashX);
        }
        return valueObj;
    }

}
