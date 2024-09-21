package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.KeyInterface;
import com.lbry.database.revert.RevertibleDelete;
import com.lbry.database.revert.RevertibleOperation;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.revert.RevertiblePut;
import com.lbry.database.util.Tuple2;
import com.lbry.database.values.ValueInterface;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public abstract class PrefixRow<K extends KeyInterface,V extends ValueInterface>{

    public static final Map<Prefix,PrefixRow<?,?>> TYPES = new HashMap<>();

    private final RocksDB database;
    private final RevertibleOperationStack operationStack;
    private final ColumnFamilyHandle columnFamily;

    public PrefixRow(RocksDB database, RevertibleOperationStack operationStack,List<ColumnFamilyHandle> columnFamilyHandles){
        this.database = database;
        this.operationStack = operationStack;
        this.columnFamily = columnFamilyHandles==null?null:columnFamilyHandles.stream().filter(x -> {
            try {
                return Arrays.equals(x.getName(),new byte[]{this.prefix().getValue()});
            } catch (RocksDBException e) {
                return false;
            }

        }).findFirst().orElse(null);

        PrefixRow.TYPES.put(this.prefix(),this);
    }

    public abstract Prefix prefix();

    public RocksIterator iterate(){
        return this.database.newIterator(this.columnFamily);
    }

    //TODO
    public RocksIterator iterate(ReadOptions readOptions) throws RocksDBException{
        return this.database.newIterator(this.columnFamily,readOptions);
    }

    public Object get(K key) throws RocksDBException{
        return this.get(key,true);
    }

    public Object get(K key,boolean fillCache) throws RocksDBException{
        return this.get(key,fillCache,true);
    }

    public Object get(K key,boolean fillCache,boolean deserializeValue) throws RocksDBException {
        ReadOptions readOptions = new ReadOptions().setFillCache(fillCache);
        byte[] v = this.database.get(this.columnFamily,readOptions,this.packKey(key));
        readOptions.close();
        if(v!=null){
            return !deserializeValue?v:this.unpackValue(v);
        }
        return null;
    }

    public boolean keyExists(K key) throws RocksDBException{
        boolean keyMayExist = this.database.keyMayExist(this.columnFamily,this.packKey(key),null);
        if(!keyMayExist){
            return false;
        }
        ReadOptions readOptions = new ReadOptions().setFillCache(true);
        boolean ret = this.database.get(readOptions,this.packKey(key))!=null;
        readOptions.close();
        return ret;
    }

    public List<Object> multiGet(List<K> keys, boolean fillCache, boolean deserializeValue) throws RocksDBException{
        List<byte[]> packedKeys = new ArrayList<>();
        for(K key : keys){
            packedKeys.add(this.packKey(key));
        }
        ReadOptions readOptions = new ReadOptions().setFillCache(fillCache);
        List<byte[]> result = this.database.multiGetAsList(readOptions,Collections.singletonList(this.columnFamily),packedKeys);
        readOptions.close();

        List<Object> ret = new ArrayList<>();
        for(int i=0;i<keys.size();i++){
            byte[] v = result.get(i);//TODO Improve???
            ret.add(v==null?null:(!deserializeValue?v:this.unpackValue(v)));
        }
        return ret;
    }

    //TODO multiGetAsyncGen

    public void stashMultiPut(Map<K,V> items){
        RevertibleOperation[] revertibleOperationArray = new RevertibleOperation[items.size()];
        int i = 0;
        for(Map.Entry<K,V> entry : items.entrySet()){
            revertibleOperationArray[i] = new RevertiblePut(this.packKey(entry.getKey()),this.packValue(entry.getValue()));
            i++;
        }
        this.operationStack.stashOperations(revertibleOperationArray);
    }

    public void stashMultiDelete(Map<K,V> items){
        RevertibleOperation[] revertibleOperationArray = new RevertibleOperation[items.size()];
        int i = 0;
        for(Map.Entry<K,V> entry : items.entrySet()){
            revertibleOperationArray[i] = new RevertibleDelete(this.packKey(entry.getKey()),this.packValue(entry.getValue()));
            i++;
        }
        this.operationStack.stashOperations(revertibleOperationArray);
    }

    public Object getPending(K key) throws RocksDBException {
        return this.getPending(key,true);
    }

    public Object getPending(K key,boolean fillCache) throws RocksDBException {
        return this.getPending(key,true,true);
    }

    public Object getPending(K key,boolean fillCache,boolean deserializeValue) throws RocksDBException{
        byte[] packedKey = this.packKey(key);
        Optional<RevertibleOperation> pendingOperation = this.operationStack.getPendingOperation(packedKey);
        if(pendingOperation.isPresent() && pendingOperation.get().isDelete()){
            return null;
        }
        byte[] v;
        if(pendingOperation.isPresent()){
            v = pendingOperation.get().getValue();
        }else{
            ReadOptions readOptions = new ReadOptions().setFillCache(fillCache);
            v = this.database.get(this.columnFamily,readOptions,packedKey);
            readOptions.close();
        }
        return v==null?null:(!deserializeValue?v:this.unpackValue(v));
    }

    public void stashPut(K key,V value){
        this.operationStack.stashOperations(new RevertibleOperation[]{
                new RevertiblePut(this.packKey(key),this.packValue(value)),
        });
    }

    public void stashDelete(K key,V value){
        this.operationStack.stashOperations(new RevertibleOperation[]{
                new RevertibleDelete(this.packKey(key),this.packValue(value)),
        });
    }

    public byte[] packPartialKey(){
        return ByteBuffer.allocate(1).put(this.prefix().getValue())/*TODO More key parts.*/.array();
    }

    public abstract byte[] packKey(K key);

    public abstract byte[] packValue(V value);

    public abstract K unpackKey(byte[] key);

    public abstract V unpackValue(byte[] value);

    public Tuple2<K,V> unpackItem(byte[] key,byte[] value){
        return new Tuple2<>(this.unpackKey(key),this.unpackValue(value));
    }

    public int estimateNumKeys() throws RocksDBException{
        return Integer.parseInt(this.database.getProperty(this.columnFamily,"rocksdb.estimate-num-keys"));
    }

}
