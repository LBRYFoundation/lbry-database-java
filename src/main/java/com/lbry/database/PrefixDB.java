package com.lbry.database;

import com.lbry.database.rows.*;

import java.util.Set;

import org.rocksdb.RocksDBException;

/**
 * Class for a revertible RocksDB database: A RocksDB database where each set of applied changes can be undone.
 */
public class PrefixDB extends BasePrefixDB{

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

    public PrefixDB(String path, int maxOpenFiles, String secondaryPath, int maxUndoDepth, Set<Byte> unsafePrefixes) throws RocksDBException{
        this(path,maxOpenFiles,secondaryPath,maxUndoDepth,unsafePrefixes,true);
    }

    public PrefixDB(String path,int maxOpenFiles,String secondaryPath,int maxUndoDepth,Set<Byte> unsafePrefixes,boolean enforceIntegrity) throws RocksDBException{
        super(path,maxOpenFiles,secondaryPath,maxUndoDepth,unsafePrefixes,enforceIntegrity);

        this.claim_to_support = new ClaimToSupportPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.support_to_claim = new SupportToClaimPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.claim_to_txo = new ClaimToTXOPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.txo_to_claim = new TXOToClaimPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.claim_to_channel = new ClaimToChannelPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.channel_to_claim = new ChannelToClaimPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.claim_short_id = new ClaimShortIDPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.claim_expiration = new ClaimExpirationPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.claim_takeover = new ClaimTakeoverPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.pending_activation = new PendingActivationPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.activated = new ActivatedPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.active_amount = new ActiveAmountPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.bid_order = new BidOrderPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.repost = new RepostPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.reposted_claim = new RepostedPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.reposted_count = new RepostedCountPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.undo = new UndoPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.utxo = new UTXOPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.hashX_utxo = new HashXUTXOPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.hashX_history = new HashXHistoryPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.block_hash = new BlockHashPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.tx_count = new TxCountPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.tx_hash = new TXHashPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.tx_num = new TXNumPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.tx = new TXPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.header = new BlockHeaderPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.touched_or_deleted = new TouchedOrDeletedPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.channel_count = new ChannelCountPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.db_state = new DBStatePrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.support_amount = new SupportAmountPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.block_txs = new BlockTxsPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.mempool_tx = new MempoolTXPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.trending_notification = new TrendingNotificationPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.touched_hashX = new TouchedHashXPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.hashX_status = new HashXStatusPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.hashX_mempool_status = new HashXMempoolStatusPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.effective_amount = new EffectiveAmountPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.future_effective_amount = new FutureEffectiveAmountPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
        this.hashX_history_hasher = new HashXHistoryHasherPrefixRow(this.database,this.operationStack,this.getColumnFamilyHandles());
    }

}