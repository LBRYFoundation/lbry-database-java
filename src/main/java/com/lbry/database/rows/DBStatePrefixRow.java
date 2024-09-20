package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.KeyInterface;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.DBState;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class DBStatePrefixRow extends PrefixRow<KeyInterface,DBState>{

    public DBStatePrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.DB_STATE;
    }

    @Override
    public byte[] packKey(KeyInterface key) {
        return new byte[]{Prefix.DB_STATE.getValue()};
    }

    @Override
    public KeyInterface unpackKey(byte[] key) {
        return KeyInterface.NULL;
    }

    @Override
    public byte[] packValue(DBState value) {
        return ByteBuffer.allocate(1+32+4+4+32+4+4+1+1+4+4+4+4+4).order(ByteOrder.BIG_ENDIAN)
                .put(this.prefix().getValue())
                .put(value.genesis)
                .putInt(value.height)
                .putInt(value.tx_count)
                .put(value.tip)
                .putInt(value.utxo_flush_count)
                .putInt(value.wall_time)
                .put(value.bit_fields)
                .put(value.db_version)
                .putInt(value.hist_flush_count)
                .putInt(value.comp_flush_count)
                .putInt(value.comp_cursor)
                .putInt(value.es_sync_height)
                .putInt(value.hashX_status_last_indexed_height)
                .array();
    }

    @Override
    public DBState unpackValue(byte[] value){
        int height = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN).position(32).getInt();
        if(value.length==94){
            value = ByteBuffer.allocate(value.length+4).order(ByteOrder.BIG_ENDIAN).put(value).putInt(height).array();
        }
        if(value.length==98){
            value = ByteBuffer.allocate(value.length+4).order(ByteOrder.BIG_ENDIAN).put(value).putInt(height).array();
        }
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        DBState valueObj = new DBState();
        valueObj.genesis = new byte[32];
        bb.get(valueObj.genesis);
        valueObj.height = bb.getInt();
        valueObj.tx_count = bb.getInt();
        valueObj.tip = new byte[32];
        bb.get(valueObj.tip);
        valueObj.utxo_flush_count = bb.getInt();
        valueObj.wall_time = bb.getInt();
        valueObj.bit_fields = bb.get();
        valueObj.db_version = bb.get();
        valueObj.hist_flush_count = bb.getInt();
        valueObj.comp_flush_count = bb.getInt();
        valueObj.comp_cursor = bb.getInt();
        valueObj.es_sync_height = bb.getInt();
        valueObj.hashX_status_last_indexed_height = bb.getInt();
        return valueObj;
    }

}
