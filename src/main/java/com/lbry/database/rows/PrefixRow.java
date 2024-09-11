package com.lbry.database.rows;

import com.lbry.database.Prefix;
import com.lbry.database.PrefixDB;
import com.lbry.database.keys.KeyInterface;
import com.lbry.database.values.ValueInterface;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public abstract class PrefixRow<K extends KeyInterface,V extends ValueInterface>{

    private final PrefixDB database;

    public PrefixRow(PrefixDB database){
        this.database = database;
    }

    public RocksIterator iterate() throws RocksDBException{
        return this.iterate(null);
    }

    public RocksIterator iterate(ReadOptions readOptions) throws RocksDBException{
        return this.database.iterator(this.getColumnFamily(),readOptions);
    }

    public Object get(K key) throws RocksDBException{
        return this.get(key,true);
    }

    public Object get(K key,boolean fillCache) throws RocksDBException{
        return this.get(key,fillCache,true);
    }

    public Object get(K key,boolean fillCache,boolean deserializeValue) throws RocksDBException {
        byte[] v = this.database.get(this.packKey(key),fillCache);
        if(v!=null){
            if(deserializeValue){
                return this.unpackValue(v);
            }
            return v;
        }
        return null;
    }

    public boolean keyExists(K key) throws RocksDBException{
        boolean keyMayExist = this.database.keyMayExist(this.packKey(key));
        if(!keyMayExist){
            return false;
        }
        return this.database.get(this.packKey(key),true)!=null;
    }

    public List<Object> multiGet(List<K> keys,boolean fillCache,boolean deserializeValue) throws RocksDBException{
        List<byte[]> result = this.database.multiGet(keys.stream().map(this::packKey).collect(Collectors.toList()),fillCache);
        return result.stream().map(v -> {
            if(v!=null){
                if(deserializeValue){
                    return this.unpackValue(v);
                }
                return v;
            }
            return null;
        }).collect(Collectors.toList());
    }

    public void stashMultiDelete(Map<K,V> items){
        Map<byte[],byte[]> map = new LinkedHashMap<>();
        for(Map.Entry<K,V> entry : items.entrySet()){
            map.put(this.packKey(entry.getKey()),this.packValue(entry.getValue()));
        }
        this.database.multiDelete(map);
    }

    public void stashMultiPut(Map<K,V> items){
        Map<byte[],byte[]> map = new LinkedHashMap<>();
        for(Map.Entry<K,V> entry : items.entrySet()){
            map.put(this.packKey(entry.getKey()),this.packValue(entry.getValue()));
        }
        this.database.multiPut(map);
    }

    public void stashDelete(K key,V value){
        this.database.stashRawDelete(this.packKey(key),this.packValue(value));
    }

    public void stashPut(K key,V value){
        this.database.stashRawPut(this.packKey(key),this.packValue(value));
    }

    public ColumnFamilyHandle getColumnFamily() throws RocksDBException{
        return this.database.getColumnFamilyByPrefix(this.prefix());
    }

    public abstract Prefix prefix();

    public abstract byte[] packKey(K key);

    public abstract byte[] packValue(V value);

    public abstract K unpackKey(byte[] key);

    public abstract V unpackValue(byte[] value);

}
