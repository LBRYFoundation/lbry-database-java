package com.lbry.database.tests;

import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ActiveAmountKey;
import com.lbry.database.keys.ClaimExpirationKey;
import com.lbry.database.keys.ClaimTakeoverKey;
import com.lbry.database.keys.KeyInterface;
import com.lbry.database.keys.TxNumKey;
import com.lbry.database.util.ArrayHelper;
import com.lbry.database.values.ActiveAmountValue;
import com.lbry.database.values.ClaimExpirationValue;
import com.lbry.database.values.ClaimTakeoverValue;
import com.lbry.database.values.TxNumValue;
import com.lbry.database.values.DBState;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiFunction;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RevertablePrefixDBTest{

    private File tmpDir;
    private PrefixDB database;

    @BeforeAll
    public void setUp() throws IOException,RocksDBException{
        this.tmpDir = Files.createTempDirectory("tmp").toFile();
        this.database = new PrefixDB(this.tmpDir.getAbsolutePath(),32);
    }

    @AfterAll
    public void tearDown(){
        this.database.close();
        this.tmpDir.deleteOnExit();
    }

    
    @Test
    public void testRollback() throws RocksDBException{
        String name = "derp";
        byte[] claimHash1 = new byte[20];
        Arrays.fill(claimHash1, (byte) 0x00);
        byte[] claimHash2 = new byte[20];
        Arrays.fill(claimHash2, (byte) 0x01);
        byte[] claimHash3 = new byte[20];
        Arrays.fill(claimHash3, (byte) 0x02);

        int takeoverHeight = 10000000;

        assertNull(this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }}));
        this.database.claim_takeover.stashPut(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claimHash1;
            this.height = takeoverHeight;
        }});
        assertNull(this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }}));
        assertEquals(10000000,((ClaimTakeoverValue)this.database.claim_takeover.getPending(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }})).height);

        this.database.commit(10000000,ArrayHelper.fill(new byte[32],(byte) 0x00));
        assertEquals(10000000,((ClaimTakeoverValue)this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }})).height);

        this.database.claim_takeover.stashDelete(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claimHash1;
            this.height = takeoverHeight;
        }});
        this.database.claim_takeover.stashPut(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claimHash2;
            this.height = takeoverHeight + 1;
        }});
        this.database.claim_takeover.stashDelete(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claimHash2;
            this.height = takeoverHeight + 1;
        }});
        this.database.commit(10000001,ArrayHelper.fill(new byte[32],(byte) 0x01));
        assertNull(this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }}));
        this.database.claim_takeover.stashPut(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claimHash3;
            this.height = takeoverHeight + 2;
        }});
        this.database.commit(10000002,ArrayHelper.fill(new byte[32],(byte) 0x02));
        assertEquals(10000002,((ClaimTakeoverValue)this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }})).height);

        this.database.claim_takeover.stashDelete(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claimHash3;
            this.height = takeoverHeight + 2;
        }});
        this.database.claim_takeover.stashPut(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claimHash2;
            this.height = takeoverHeight + 3;
        }});
        this.database.commit(10000003,ArrayHelper.fill(new byte[32],(byte) 0x03));
        assertEquals(10000003,((ClaimTakeoverValue)this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }})).height);

        this.database.rollback(10000003,ArrayHelper.fill(new byte[32],(byte) 0x03));
        assertEquals(10000002,((ClaimTakeoverValue)this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }})).height);
        this.database.rollback(10000002,ArrayHelper.fill(new byte[32],(byte) 0x02));
        assertNull(this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }}));
        this.database.rollback(10000001,ArrayHelper.fill(new byte[32],(byte) 0x01));
        assertEquals(10000000,((ClaimTakeoverValue)this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }})).height);
        this.database.rollback(10000000,ArrayHelper.fill(new byte[32],(byte) 0x00));
        assertNull(this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }}));
    }

    @Test
    public void testHubDatabaseIterator() throws RocksDBException{
        String name = "derp";
        byte[] claimHash0 = new byte[20];
        Arrays.fill(claimHash0, (byte) 0x00);
        byte[] claimHash1 = new byte[20];
        Arrays.fill(claimHash1, (byte) 0x01);
        byte[] claimHash2 = new byte[20];
        Arrays.fill(claimHash2, (byte) 0x02);
        byte[] claimHash3 = new byte[20];
        Arrays.fill(claimHash3, (byte) 0x02);
        int overflowValue = 0xFFFFFFFF;

        this.database.claim_expiration.stashPut(new ClaimExpirationKey(){{
            this.expiration = 99;
            this.tx_num = 999;
            this.position = 0;
        }},new ClaimExpirationValue(){{
            this.claim_hash = claimHash0;
            this.normalized_name = name;
        }});
        this.database.claim_expiration.stashPut(new ClaimExpirationKey(){{
            this.expiration = 100;
            this.tx_num = 1000;
            this.position = 0;
        }},new ClaimExpirationValue(){{
            this.claim_hash = claimHash1;
            this.normalized_name = name;
        }});
        this.database.claim_expiration.stashPut(new ClaimExpirationKey(){{
            this.expiration = 100;
            this.tx_num = 1001;
            this.position = 0;
        }},new ClaimExpirationValue(){{
            this.claim_hash = claimHash2;
            this.normalized_name = name;
        }});
        this.database.claim_expiration.stashPut(new ClaimExpirationKey(){{
            this.expiration = 101;
            this.tx_num = 1002;
            this.position = 0;
        }},new ClaimExpirationValue(){{
            this.claim_hash = claimHash3;
            this.normalized_name = name;
        }});
        this.database.claim_expiration.stashPut(new ClaimExpirationKey(){{
            this.expiration = overflowValue-1;
            this.tx_num = 1003;
            this.position = 0;
        }},new ClaimExpirationValue(){{
            this.claim_hash = claimHash3;
            this.normalized_name = name;
        }});
        this.database.claim_expiration.stashPut(new ClaimExpirationKey(){{
            this.expiration = overflowValue;
            this.tx_num = 1004;
            this.position = 0;
        }},new ClaimExpirationValue(){{
            this.claim_hash = claimHash3;
            this.normalized_name = name;
        }});
        this.database.tx_num.stashPut(new TxNumKey(){{
            this.tx_hash = new byte[32];
            Arrays.fill(this.tx_hash, (byte) 0x00);
        }},new TxNumValue(){{
            this.tx_num = 101;
        }});
        this.database.claim_takeover.stashPut(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claimHash3;
            this.height = 101;
        }});

        this.database.db_state.stashPut(KeyInterface.NULL,new DBState(){{
            this.genesis = new byte[]{'n','?',(byte) 0xcf,0x12,(byte) 0x99,(byte) 0xd4,(byte) 0xec,']','y',(byte) 0xc3,(byte) 0xa4,(byte) 0xc9,0x1d,'b','J','J',(byte) 0xcf,(byte) 0x9e,'.',0x17,'=',(byte) 0x95,(byte) 0xa1,(byte) 0xa0,'P','O','g','v','i','h','u','V'};
            this.height = 0;
            this.tx_count = 1;
            this.tip = new byte[]{'V','u','h','i','v','g','O','P',(byte) 0xa0,(byte) 0xa1,(byte) 0x95,'=',0x17,'.',(byte) 0x9e,(byte) 0xcf,'J','J','b',0x1d,(byte) 0xc9,(byte) 0xa4,(byte) 0xc3,'y',']',(byte) 0xec,(byte) 0xd4,(byte) 0x99,0x12,(byte) 0xcf,'?','n'};
            this.utxo_flush_count = 1;
            this.wall_time = 0;
            this.bit_fields = 1;
            this.db_version = 7;
            this.hist_flush_count = 1;
            this.comp_flush_count = -1;
            this.comp_cursor = -1;
            this.es_sync_height = 0;
            this.hashX_status_last_indexed_height = 0;
            // 0
        }});
        this.database.unsafeCommit();

        DBState state = (DBState) this.database.db_state.get(KeyInterface.NULL);
        assertArrayEquals(new byte[]{'n','?',(byte) 0xcf,0x12,(byte) 0x99,(byte) 0xd4,(byte) 0xec,']','y',(byte) 0xc3,(byte) 0xa4,(byte) 0xc9,0x1d,'b','J','J',(byte) 0xcf,(byte) 0x9e,'.',0x17,'=',(byte) 0x95,(byte) 0xa1,(byte) 0xa0,'P','O','g','v','i','h','u','V'},state.genesis);

        {
            Map<byte[],byte[]> actualMap = new HashMap<>();
            RocksIterator iterator = this.database.claim_expiration.iterate();
            iterator.seekToFirst();
            while(iterator.isValid()){
                if(this.database.claim_expiration.unpackKey(iterator.key()).expiration==98){
                    actualMap.put(iterator.key(),iterator.value());
                }
                iterator.next();
            }
            iterator.close();
            assertEquals(0,actualMap.size());
        }
        //TODO: (start=98 & stop=99) vs (prefix=98)
        //TODO: (start=99 & stop=100) vs (prefix=99)
        {
            Map<byte[],byte[]> expectedMap = new HashMap<>();
            expectedMap.put(this.database.claim_expiration.packKey(new ClaimExpirationKey(){{
                this.expiration = 99;
                this.tx_num = 999;
                this.position = 0;
            }}),this.database.claim_expiration.packValue(new ClaimExpirationValue(){{
                this.claim_hash = claimHash0;
                this.normalized_name = name;
            }}));
            Map<byte[],byte[]> actualMap = new HashMap<>();
            RocksIterator iterator = this.database.claim_expiration.iterate();
            iterator.seekToFirst();
            while(iterator.isValid()){
                if(this.database.claim_expiration.unpackKey(iterator.key()).expiration==99){
                    actualMap.put(iterator.key(),iterator.value());
                }
                iterator.next();
            }
            assertEquals(expectedMap.size(),actualMap.size());
            Iterator<Map.Entry<byte[],byte[]>> expectedEntrySetIterator = expectedMap.entrySet().iterator();
            Iterator<Map.Entry<byte[],byte[]>> actualEntrySetIterator = actualMap.entrySet().iterator();
            for(int i=0;i<expectedMap.size();i++){
                Map.Entry<byte[],byte[]> expectedEntry = expectedEntrySetIterator.next();
                Map.Entry<byte[],byte[]> actualEntry = actualEntrySetIterator.next();
                assertArrayEquals(expectedEntry.getKey(),actualEntry.getKey());
                assertArrayEquals(expectedEntry.getValue(),actualEntry.getValue());
            }
            iterator.close();
        }
        {
            Map<byte[],byte[]> expectedMap = new HashMap<>();
            expectedMap.put(this.database.claim_expiration.packKey(new ClaimExpirationKey(){{
                this.expiration = 100;
                this.tx_num = 1000;
                this.position = 0;
            }}),this.database.claim_expiration.packValue(new ClaimExpirationValue(){{
                this.claim_hash = claimHash1;
                this.normalized_name = name;
            }}));
            expectedMap.put(this.database.claim_expiration.packKey(new ClaimExpirationKey(){{
                this.expiration = 100;
                this.tx_num = 1001;
                this.position = 0;
            }}),this.database.claim_expiration.packValue(new ClaimExpirationValue(){{
                this.claim_hash = claimHash2;
                this.normalized_name = name;
            }}));
            Map<byte[],byte[]> actualMap = new HashMap<>();
            RocksIterator iterator = this.database.claim_expiration.iterate();
            iterator.seekToFirst();
            while(iterator.isValid()){
                if(this.database.claim_expiration.unpackKey(iterator.key()).expiration==100){
                    actualMap.put(iterator.key(),iterator.value());
                }
                iterator.next();
            }
            assertEquals(expectedMap.size(),actualMap.size());
            Iterator<Map.Entry<byte[],byte[]>> expectedEntrySetIterator = expectedMap.entrySet().iterator();
            Iterator<Map.Entry<byte[],byte[]>> actualEntrySetIterator = actualMap.entrySet().iterator();
            for(int i=0;i<expectedMap.size();i++){
                Map.Entry<byte[],byte[]> expectedEntry = expectedEntrySetIterator.next();
                Map.Entry<byte[],byte[]> actualEntry = actualEntrySetIterator.next();
                assertArrayEquals(expectedEntry.getKey(),actualEntry.getKey());
                assertArrayEquals(expectedEntry.getValue(),actualEntry.getValue());
            }
            iterator.close();
        }
        //TODO (start=100 & stop=101) vs (prefix=100)
        {
            Map<byte[],byte[]> expectedMap = new HashMap<>();
            expectedMap.put(this.database.claim_expiration.packKey(new ClaimExpirationKey(){{
                this.expiration = overflowValue-1;
                this.tx_num = 1003;
                this.position = 0;
            }}),this.database.claim_expiration.packValue(new ClaimExpirationValue(){{
                this.claim_hash = claimHash3;
                this.normalized_name = name;
            }}));
            Map<byte[],byte[]> actualMap = new HashMap<>();
            RocksIterator iterator = this.database.claim_expiration.iterate();
            iterator.seekToFirst();
            while(iterator.isValid()){
                if(this.database.claim_expiration.unpackKey(iterator.key()).expiration==overflowValue-1){
                    actualMap.put(iterator.key(),iterator.value());
                }
                iterator.next();
            }
            assertEquals(expectedMap.size(),actualMap.size());
            Iterator<Map.Entry<byte[],byte[]>> expectedEntrySetIterator = expectedMap.entrySet().iterator();
            Iterator<Map.Entry<byte[],byte[]>> actualEntrySetIterator = actualMap.entrySet().iterator();
            for(int i=0;i<expectedMap.size();i++){
                Map.Entry<byte[],byte[]> expectedEntry = expectedEntrySetIterator.next();
                Map.Entry<byte[],byte[]> actualEntry = actualEntrySetIterator.next();
                assertArrayEquals(expectedEntry.getKey(),actualEntry.getKey());
                assertArrayEquals(expectedEntry.getValue(),actualEntry.getValue());
            }
            iterator.close();
        }
        {
            Map<byte[],byte[]> expectedMap = new HashMap<>();
            expectedMap.put(this.database.claim_expiration.packKey(new ClaimExpirationKey(){{
                this.expiration = overflowValue;
                this.tx_num = 1004;
                this.position = 0;
            }}),this.database.claim_expiration.packValue(new ClaimExpirationValue(){{
                this.claim_hash = claimHash3;
                this.normalized_name = name;
            }}));
            Map<byte[],byte[]> actualMap = new HashMap<>();
            RocksIterator iterator = this.database.claim_expiration.iterate();
            iterator.seekToFirst();
            while(iterator.isValid()){
                if(this.database.claim_expiration.unpackKey(iterator.key()).expiration==overflowValue){
                    actualMap.put(iterator.key(),iterator.value());
                }
                iterator.next();
            }
            assertEquals(expectedMap.size(),actualMap.size());
            Iterator<Map.Entry<byte[],byte[]>> expectedEntrySetIterator = expectedMap.entrySet().iterator();
            Iterator<Map.Entry<byte[],byte[]>> actualEntrySetIterator = actualMap.entrySet().iterator();
            for(int i=0;i<expectedMap.size();i++){
                Map.Entry<byte[],byte[]> expectedEntry = expectedEntrySetIterator.next();
                Map.Entry<byte[],byte[]> actualEntry = actualEntrySetIterator.next();
                assertArrayEquals(expectedEntry.getKey(),actualEntry.getKey());
                assertArrayEquals(expectedEntry.getValue(),actualEntry.getValue());
            }
            iterator.close();
        }
    }

    @Test
    public void testHubDatabaseIteratorStartStop() throws RocksDBException{
        int txNum = 101;

        for(int x=0;x<255;x++){
            byte[] claimHash = ArrayHelper.fill(new byte[20],(byte) x);
            final int txNumInner = txNum;
            this.database.active_amount.stashPut(new ActiveAmountKey(){{
                this.claim_hash = claimHash;
                this.txo_type = 1;
                this.activation_height = 200;
                this.tx_num = txNumInner;
                this.position = 1;
            }},new ActiveAmountValue(){{
                this.amount = 100000;
            }});
            this.database.active_amount.stashPut(new ActiveAmountKey(){{
                this.claim_hash = claimHash;
                this.txo_type = 1;
                this.activation_height = 201;
                this.tx_num = txNumInner+1;
                this.position = 1;
            }},new ActiveAmountValue(){{
                this.amount = 200000;
            }});
            this.database.active_amount.stashPut(new ActiveAmountKey(){{
                this.claim_hash = claimHash;
                this.txo_type = 1;
                this.activation_height = 202;
                this.tx_num = txNumInner+2;
                this.position = 1;
            }},new ActiveAmountValue(){{
                this.amount = 300000;
            }});
            txNum += 3;
        }
        this.database.unsafeCommit();

        BiFunction<byte[],Integer,Long> getActiveAmountAsOfHeight = (claimHash,height) -> {
            try{
                ReadOptions readOptions = new ReadOptions().setPrefixSameAsStart(true).setTotalOrderSeek(true);
                RocksIterator iterator = this.database.active_amount.iterate(readOptions);
                iterator.seek(ByteBuffer.allocate(1+20+1+4).order(ByteOrder.BIG_ENDIAN).put(this.database.active_amount.prefix().getValue()).put(claimHash).put((byte) 1).putInt(0));
                byte[] stop = ByteBuffer.allocate(1+20+1+4).order(ByteOrder.BIG_ENDIAN).put(this.database.active_amount.prefix().getValue()).put(claimHash).put((byte) 1).putInt(height).array();
                byte[] latestValue = null;
                while(iterator.isValid()){
                    if(ByteBuffer.wrap(iterator.key(),0,1+20+1).equals(ByteBuffer.wrap(stop,0,1+20+1))){
                        int compareStop = ByteBuffer.wrap(iterator.key(),0,1+20+1+4).compareTo(ByteBuffer.wrap(stop));
                        if(compareStop>0){
                            break;
                        }
                        latestValue = iterator.value();
                    }
                    iterator.next();
                }
                return latestValue!=null?this.database.active_amount.unpackValue(latestValue).amount:0;
            }catch(RocksDBException e){
                e.printStackTrace();
            }
            return 0L;
        };

        for(int x=0;x<255;x++){
            byte[] claimHash = ArrayHelper.fill(new byte[20],(byte) x);

            assertEquals(300000,getActiveAmountAsOfHeight.apply(claimHash,300));
            assertEquals(300000,getActiveAmountAsOfHeight.apply(claimHash,203));
            assertEquals(300000,getActiveAmountAsOfHeight.apply(claimHash,202));
            assertEquals(200000,getActiveAmountAsOfHeight.apply(claimHash,201));
            assertEquals(100000,getActiveAmountAsOfHeight.apply(claimHash,200));
            assertEquals(0,getActiveAmountAsOfHeight.apply(claimHash,199));
        }
    }

}