package com.lbry.database;

import com.lbry.database.keys.UndoKey;
import com.lbry.database.revert.RevertibleDelete;
import com.lbry.database.revert.RevertibleOperation;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.revert.RevertiblePut;
import com.lbry.database.rows.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import org.rocksdb.*;

/**
 * Class for a revertible RocksDB database: A RocksDB database where each set of applied changes can be undone.
 */
public class PrefixDB{

    private final List<ColumnFamilyHandle> columnFamilyHandles;
    private final RocksDB database;
    private final RevertibleOperationStack operationStack;
    private final int maxUndoDepth;

    public final ClaimToSupportPrefixRow claim_to_support;
    public final SupportToClaimPrefixRow support_to_claim;
    public final ClaimToTXOPrefixRow claim_to_txo;
    public final TXOToClaimPrefixRow txo_to_claim;
    public final ClaimToChannelPrefixRow claim_to_channel;
    public final ChannelToClaimPrefixRow channel_to_claim;
    public final ClaimShortIDPrefixRow claim_short_id;
    public final ClaimExpirationPrefixRow claim_expiration;
    public final ClaimTakeoverPrefixRow claim_takeover;
    public final PendingActivationPrefixRow pending_activation;
    public final ActivatedPrefixRow activated;
    public final ActiveAmountPrefixRow active_amount;
    public final BidOrderPrefixRow bid_order;
    public final RepostPrefixRow repost;
    public final RepostedPrefixRow reposted_claim;
    public final RepostedCountPrefixRow reposted_count;
    public final UndoPrefixRow undo;
    public final UTXOPrefixRow utxo;
    public final HashXUTXOPrefixRow hashX_utxo;
    public final HashXHistoryPrefixRow hashX_history;
    public final BlockHashPrefixRow block_hash;
    public final TxCountPrefixRow tx_count;
    public final TXHashPrefixRow tx_hash;
    public final TXNumPrefixRow tx_num;
    public final TXPrefixRow tx;
    public final BlockHeaderPrefixRow header;
    public final TouchedOrDeletedPrefixRow touched_or_deleted;
    public final ChannelCountPrefixRow channel_count;
    public final DBStatePrefixRow db_state;
    public final SupportAmountPrefixRow support_amount;
    public final BlockTxsPrefixRow block_txs;
    public final MempoolTXPrefixRow mempool_tx;
    public final TrendingNotificationPrefixRow trending_notification;
    public final TouchedHashXPrefixRow touched_hashX;
    public final HashXStatusPrefixRow hashX_status;
    public final HashXMempoolStatusPrefixRow hashX_mempool_status;
    public final EffectiveAmountPrefixRow effective_amount;
    public final FutureEffectiveAmountPrefixRow future_effective_amount;
    public final HashXHistoryHasherPrefixRow hashX_history_hasher;

    public PrefixDB(String path) throws RocksDBException {
        this(path,64);
    }

    public PrefixDB(String path,int maxOpenFiles) throws RocksDBException{
        this(path,maxOpenFiles,null);
    }

    public PrefixDB(String path,int maxOpenFiles,String secondaryPath) throws RocksDBException{
        this(path,maxOpenFiles,secondaryPath,200);
    }

    public PrefixDB(String path,int maxOpenFiles,String secondaryPath,int maxUndoDepth) throws RocksDBException{
        this(path,maxOpenFiles,secondaryPath,maxUndoDepth,null);
    }

    public PrefixDB(String path,int maxOpenFiles,String secondaryPath,int maxUndoDepth,Set<Byte> unsafePrefixes) throws RocksDBException{
        this(path,maxOpenFiles,secondaryPath,maxUndoDepth,unsafePrefixes,true);
    }

    public PrefixDB(String path,int maxOpenFiles,String secondaryPath,int maxUndoDepth,Set<Byte> unsafePrefixes,boolean enforceIntegrity) throws RocksDBException{
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

        this.claim_to_support = new ClaimToSupportPrefixRow(this,this.operationStack);
        this.support_to_claim = new SupportToClaimPrefixRow(this,this.operationStack);
        this.claim_to_txo = new ClaimToTXOPrefixRow(this,this.operationStack);
        this.txo_to_claim = new TXOToClaimPrefixRow(this,this.operationStack);
        this.claim_to_channel = new ClaimToChannelPrefixRow(this,this.operationStack);
        this.channel_to_claim = new ChannelToClaimPrefixRow(this,this.operationStack);
        this.claim_short_id = new ClaimShortIDPrefixRow(this,this.operationStack);
        this.claim_expiration = new ClaimExpirationPrefixRow(this,this.operationStack);
        this.claim_takeover = new ClaimTakeoverPrefixRow(this,this.operationStack);
        this.pending_activation = new PendingActivationPrefixRow(this,this.operationStack);
        this.activated = new ActivatedPrefixRow(this,this.operationStack);
        this.active_amount = new ActiveAmountPrefixRow(this,this.operationStack);
        this.bid_order = new BidOrderPrefixRow(this,this.operationStack);
        this.repost = new RepostPrefixRow(this,this.operationStack);
        this.reposted_claim = new RepostedPrefixRow(this,this.operationStack);
        this.reposted_count = new RepostedCountPrefixRow(this,this.operationStack);
        this.undo = new UndoPrefixRow(this,this.operationStack);
        this.utxo = new UTXOPrefixRow(this,this.operationStack);
        this.hashX_utxo = new HashXUTXOPrefixRow(this,this.operationStack);
        this.hashX_history = new HashXHistoryPrefixRow(this,this.operationStack);
        this.block_hash = new BlockHashPrefixRow(this,this.operationStack);
        this.tx_count = new TxCountPrefixRow(this,this.operationStack);
        this.tx_hash = new TXHashPrefixRow(this,this.operationStack);
        this.tx_num = new TXNumPrefixRow(this,this.operationStack);
        this.tx = new TXPrefixRow(this,this.operationStack);
        this.header = new BlockHeaderPrefixRow(this,this.operationStack);
        this.touched_or_deleted = new TouchedOrDeletedPrefixRow(this,this.operationStack);
        this.channel_count = new ChannelCountPrefixRow(this,this.operationStack);
        this.db_state = new DBStatePrefixRow(this,this.operationStack);
        this.support_amount = new SupportAmountPrefixRow(this,this.operationStack);
        this.block_txs = new BlockTxsPrefixRow(this,this.operationStack);
        this.mempool_tx = new MempoolTXPrefixRow(this,this.operationStack);
        this.trending_notification = new TrendingNotificationPrefixRow(this,this.operationStack);
        this.touched_hashX = new TouchedHashXPrefixRow(this,this.operationStack);
        this.hashX_status = new HashXStatusPrefixRow(this,this.operationStack);
        this.hashX_mempool_status = new HashXMempoolStatusPrefixRow(this,this.operationStack);
        this.effective_amount = new EffectiveAmountPrefixRow(this,this.operationStack);
        this.future_effective_amount = new FutureEffectiveAmountPrefixRow(this,this.operationStack);
        this.hashX_history_hasher = new HashXHistoryHasherPrefixRow(this,this.operationStack);
    }

    /**
     * Write staged changes to the database without keeping undo information. Changes written cannot be undone.
     */
    public void unsafeCommit() throws RocksDBException{
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
                byte[] undoKeyBytes = this.undo.packKey(undoKey);
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
        byte[] undoKeyBytes = this.undo.packKey(undoKey);
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