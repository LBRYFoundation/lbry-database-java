package com.lbry.database;

import com.lbry.database.keys.UndoKey;
import com.lbry.database.revert.RevertibleDelete;
import com.lbry.database.revert.RevertibleOperation;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.revert.RevertiblePut;

import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * Base class for a revertible RocksDB database (a RocksDB database where each set of applied changes can be undone).
 */
public abstract class BasePrefixDB{

    private final List<ColumnFamilyHandle> columnFamilyHandles;
    protected final RocksDB database;
    protected final RevertibleOperationStack operationStack;
    private final int maxUndoDepth;

    public BasePrefixDB(String path, int maxOpenFiles, String secondaryPath, int maxUndoDepth, Set<Byte> unsafePrefixes, boolean enforceIntegrity) throws RocksDBException{
        List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        for(Prefix prefix : Prefix.values()){
            byte[] name = new byte[]{prefix.getValue()};
            ColumnFamilyDescriptor descriptor = new ColumnFamilyDescriptor(name);
            columnFamilyDescriptors.add(descriptor);
        }

        this.columnFamilyHandles = new ArrayList<>();

        Options options = new Options()
                .setCreateIfMissing(true)
                .setUseFsync(false)
                .setTargetFileSizeBase(33554432)
                .setMaxOpenFiles(secondaryPath==null?maxOpenFiles:-1)
                .setCreateMissingColumnFamilies(true);

        this.database = RocksDB.open(new DBOptions(options),path,columnFamilyDescriptors,this.columnFamilyHandles);

        this.operationStack = new RevertibleOperationStack((byte[] key) -> {
            try{
                return Optional.of(this.get(key));
            }catch(RocksDBException e){}
            return Optional.empty();
        },(List<byte[]> keys) -> {
            List<Optional<byte[]>> optionalKeys = new ArrayList<>();
            for(byte[] key : keys){
                optionalKeys.add(Optional.of(key));
            }
            return optionalKeys;
        },unsafePrefixes,enforceIntegrity);

        this.maxUndoDepth = maxUndoDepth;
    }

    /**
     * Write staged changes to the database without keeping undo information. Changes written cannot be undone.
     */


    public void unsafeCommit() throws RocksDBException {
        this.applyStash();
        WriteOptions writeOptions = new WriteOptions().setSync(true);
        try{
            if(this.operationStack.length()!=0){
                return;
            }
            WriteBatch batch = new WriteBatch();
            for(RevertibleOperation stagedChange : this.operationStack.iterate()){
                ColumnFamilyHandle columnFamily = this.getColumnFamilyByPrefix(Prefix.getByValue(stagedChange.getKey()[0]));
                if(!stagedChange.isDelete()){
                    batch.put(columnFamily,stagedChange.getKey(),stagedChange.getValue());
                }else{
                    batch.delete(columnFamily,stagedChange.getKey());
                }
                this.database.write(writeOptions,batch);
            }
        }finally{
            writeOptions.close();
            this.operationStack.clear();
        }
    }

    public void commit(int height,byte[] blockHash) throws RocksDBException{
        this.applyStash();
        byte[] undoOperations = this.operationStack.getUndoOperations();
        List<byte[]> deleteUndos = new ArrayList<>();
        if(height>this.maxUndoDepth){
            byte[] upperBound = ByteBuffer.allocate(1+8).order(ByteOrder.BIG_ENDIAN).put(Prefix.UNDO.getValue()).putLong(height-this.maxUndoDepth).array();
            RocksIterator iterator = this.database.newIterator(new ReadOptions().setIterateUpperBound(new Slice(upperBound)));
            iterator.seek(ByteBuffer.allocate(1+8).order(ByteOrder.BIG_ENDIAN).put(Prefix.UNDO.getValue()).array());
            while(iterator.isValid()){
                deleteUndos.add(iterator.key());
                iterator.next();
            }
        }
        try{
            ColumnFamilyHandle undoColumnFamily = this.getColumnFamilyByPrefix(Prefix.UNDO);
            WriteOptions writeOptions = new WriteOptions().setSync(true);
            try{
                WriteBatch batch = new WriteBatch();
                for(RevertibleOperation stagedChange : this.operationStack.iterate()){
                    ColumnFamilyHandle columnFamily = this.getColumnFamilyByPrefix(Prefix.getByValue(stagedChange.getKey()[0]));
                    if(!stagedChange.isDelete()){
                        batch.put(columnFamily,stagedChange.getKey(),stagedChange.getValue());
                    }else{
                        batch.delete(columnFamily,stagedChange.getKey());
                    }

                }
                for(byte[] undoToDelete : deleteUndos){
                    batch.delete(undoColumnFamily,undoToDelete);
                }
                UndoKey undoKey = new UndoKey();
                undoKey.height = height;
                undoKey.block_hash = blockHash;
                byte[] undoKeyBytes = ((PrefixDB)this).undo.packKey(undoKey);
                batch.put(undoColumnFamily,undoKeyBytes,undoOperations);
                this.database.write(writeOptions,batch);
            }finally{
                writeOptions.close();
                this.operationStack.clear();
            }
        }finally{
            this.operationStack.clear();
        }
    }

    public void rollback(int height,byte[] blockHash) throws RocksDBException{
        UndoKey undoKey = new UndoKey();
        undoKey.height = height;
        undoKey.block_hash = blockHash;
        byte[] undoKeyBytes = ((PrefixDB)this).undo.packKey(undoKey);
        ColumnFamilyHandle undoColumnFamily = this.getColumnFamilyByPrefix(Prefix.UNDO);
        byte[] undoInfo = this.database.get(undoColumnFamily,undoKeyBytes);
        this.operationStack.applyPackedUndoOperations(undoInfo);
        this.operationStack.validateAndApplyStashedOperations();
        WriteOptions writeOptions = new WriteOptions().setSync(true);
        try{
            WriteBatch batch = new WriteBatch();
            for(RevertibleOperation stagedChange : this.operationStack.iterate()){
                ColumnFamilyHandle columnFamily = this.getColumnFamilyByPrefix(Prefix.getByValue(stagedChange.getKey()[0]));
                if(!stagedChange.isDelete()){
                    batch.put(columnFamily,stagedChange.getKey(),stagedChange.getValue());
                }else{
                    batch.delete(columnFamily,stagedChange.getKey());
                }
                this.database.write(writeOptions,batch);
            }
            // batch.delete(undoKey)
        }finally{
            writeOptions.close();
            this.operationStack.clear();
        }
    }

    public void applyStash(){
        this.operationStack.validateAndApplyStashedOperations();
    }

    /**
     * Get value by prefixed key.
     * @param key The prefixed key.
     * @return The value or null.
     * @throws RocksDBException The exception.
     */
    public byte[] get(byte[] key) throws RocksDBException{
        return this.get(key,true);
    }

    /**
     * Get value by prefixed key.
     * @param key The prefixed key.
     * @param fillCache Fill cache option.
     * @return The value or null.
     * @throws RocksDBException The exception.
     */
    public byte[] get(byte[] key,boolean fillCache) throws RocksDBException{
        ColumnFamilyHandle columnFamily = null;
        for(ColumnFamilyHandle handle : this.columnFamilyHandles){
            if(key.length>0 && Arrays.equals(handle.getName(),new byte[]{key[0]})){
                columnFamily = handle;
                break;
            }
        }
        ReadOptions options = new ReadOptions().setFillCache(fillCache);
        byte[] value = this.database.get(columnFamily,options,key);
        options.close();
        return value;
    }

    /**
     * Get multiple values by prefixed keys.
     * @param keys The prefixed keys.
     * @return The values.
     * @throws RocksDBException The exception.
     */
    public List<byte[]> multiGet(List<byte[]> keys) throws RocksDBException{
        return this.multiGet(keys,true);
    }

    /**
     * Get multiple values by prefixed keys.
     * @param keys The prefixed keys.
     * @param fillCache Fill cache option.
     * @return The values.
     * @throws RocksDBException The exception.
     */
    public List<byte[]> multiGet(List<byte[]> keys,boolean fillCache) throws RocksDBException{
        List<ColumnFamilyHandle> columnFamilies = new ArrayList<>();
        for(byte[] key : keys){
            for(ColumnFamilyHandle handle : this.columnFamilyHandles){
                if(Arrays.equals(handle.getName(),new byte[]{key[0]})){
                    columnFamilies.add(handle);
                    break;
                }
            }
        }
        ReadOptions options = new ReadOptions().setFillCache(fillCache);
        List<byte[]> values = this.database.multiGetAsList(options,columnFamilies,keys);
        options.close();
        return values;
    }

    /**
     * Stash multiple items for deletion.
     * @param items The items.
     */
    public void multiDelete(Map<byte[],byte[]> items){
        this.operationStack.stashOperations(items.entrySet().stream().map((entry) -> new RevertibleDelete(entry.getKey(),entry.getValue())).toArray(RevertibleOperation[]::new));
    }

    /**
     * Stash multiple items for putting.
     * @param items The items.
     */
    public void multiPut(Map<byte[],byte[]> items){
        this.operationStack.stashOperations(items.entrySet().stream().map((entry) -> new RevertiblePut(entry.getKey(),entry.getValue())).toArray(RevertibleOperation[]::new));
    }

    public RocksIterator iterator(){
        return this.iterator(null,null);
    }

    public RocksIterator iterator(ReadOptions readOptions){
        return this.iterator(null,readOptions);
    }

    public RocksIterator iterator(ColumnFamilyHandle columnFamily){
        return this.iterator(columnFamily,null);
    }

    public RocksIterator iterator(ColumnFamilyHandle columnFamily,ReadOptions readOptions){
        if(columnFamily==null && readOptions==null){
            return this.database.newIterator();
        }
        if(columnFamily==null){
            return this.database.newIterator(readOptions);
        }
        if(readOptions==null){
            return this.database.newIterator(columnFamily);
        }
        return this.database.newIterator(columnFamily,readOptions);
    }

    /**
     * Close database.
     */
    public void close(){
        this.database.close();
    }

    public void tryCatchUpWithPrimary() throws RocksDBException{
        this.database.tryCatchUpWithPrimary();
    }

    /**
     * Stash item for deletion.
     * @param key The item prefixed key.
     * @param value The value.
     */
    public void stashRawDelete(byte[] key,byte[] value){
        this.operationStack.stashOperations(new RevertibleOperation[]{
                new RevertibleDelete(key,value),
        });
    }

    /**
     * Stash item for putting.
     * @param key The item prefixed key.
     * @param value The value.
     */
    public void stashRawPut(byte[] key,byte[] value){
        this.operationStack.stashOperations(new RevertibleOperation[]{
                new RevertiblePut(key,value),
        });
    }

    public int estimateNumKeys() throws RocksDBException{
        return this.estimateNumKeys(null);
    }

    public int estimateNumKeys(ColumnFamilyHandle columnFamily) throws RocksDBException{
        return Integer.parseInt(this.database.getProperty(columnFamily,"rocksdb.estimate-num-keys"));
    }


    public boolean keyMayExist(byte[] key) throws RocksDBException{
        ColumnFamilyHandle columnFamily = null;
        for(ColumnFamilyHandle handle : this.columnFamilyHandles){
            if(key.length>0 && Arrays.equals(handle.getName(),new byte[]{key[0]})){
                columnFamily = handle;
                break;
            }
        }
        return this.database.keyMayExist(columnFamily,ByteBuffer.wrap(key));
    }

    public List<ColumnFamilyHandle> getColumnFamilyHandles(){
        return new ArrayList<>(this.columnFamilyHandles);
    }

    public ColumnFamilyHandle getColumnFamilyByPrefix(Prefix prefix) throws RocksDBException{
        if(prefix==null){
            return this.database.getDefaultColumnFamily();
        }
        for(ColumnFamilyHandle columnFamily : this.columnFamilyHandles){
            if(Arrays.equals(columnFamily.getName(),new byte[]{prefix.getValue()})){
                return columnFamily;
            }
        }
        return null;
    }

}