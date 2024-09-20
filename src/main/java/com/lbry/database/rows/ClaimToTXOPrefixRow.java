package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ClaimToTXOKey;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.values.ClaimToTXOValue;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

public class ClaimToTXOPrefixRow extends PrefixRow<ClaimToTXOKey,ClaimToTXOValue>{

    public ClaimToTXOPrefixRow(RocksDB database, RevertibleOperationStack operationStack, List<ColumnFamilyHandle> columnFamilyHandleList){
        super(database,operationStack,columnFamilyHandleList);
    }

    @Override
    public Prefix prefix(){
        return Prefix.CLAIM_TO_TXO;
    }

    @Override
    public byte[] packKey(ClaimToTXOKey key) {
        return ByteBuffer.allocate(1+20).order(ByteOrder.BIG_ENDIAN).put(this.prefix().getValue()).put(key.claim_hash).array();
    }

    @Override
    public ClaimToTXOKey unpackKey(byte[] key) {
        ByteBuffer bb = ByteBuffer.wrap(key).order(ByteOrder.BIG_ENDIAN);
        if(bb.get()!=this.prefix().getValue()){
            return null;
        }
        ClaimToTXOKey keyObj = new ClaimToTXOKey();
        keyObj.claim_hash = new byte[20];
        bb.get(keyObj.claim_hash);
        return keyObj;
    }

    @Override
    public byte[] packValue(ClaimToTXOValue value) {
        byte[] strBytes = value.name.getBytes();

        return ByteBuffer.allocate(4+2+4+2+8+1+2+strBytes.length)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(value.tx_num)
                .putShort(value.position)
                .putInt(value.root_tx_num)
                .putShort(value.root_position)
                .putLong(value.amount)
                .put((byte) (value.channel_signature_is_valid?0x01:0x00))
                .putShort((short) strBytes.length)
                .put(strBytes)
                .array();
    }

    @Override
    public ClaimToTXOValue unpackValue(byte[] value) {
        ByteBuffer bb = ByteBuffer.wrap(value).order(ByteOrder.BIG_ENDIAN);
        ClaimToTXOValue valueObj = new ClaimToTXOValue();
        valueObj.tx_num = bb.getInt();
        valueObj.position = bb.getShort();
        valueObj.root_tx_num = bb.getInt();
        valueObj.root_position = bb.getShort();
        valueObj.amount = bb.getLong();
        valueObj.channel_signature_is_valid = bb.get()!=0x00;
        byte[] strBytes = new byte[bb.getShort()];
        bb.get(strBytes);
        valueObj.name = new String(strBytes);
        return valueObj;
    }

}
