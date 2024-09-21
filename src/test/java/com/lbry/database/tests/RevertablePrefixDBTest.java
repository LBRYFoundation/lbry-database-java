package com.lbry.database.tests;

import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ActiveAmountKey;
import com.lbry.database.keys.ClaimTakeoverKey;
import com.lbry.database.revert.RevertibleOperation;
import com.lbry.database.revert.RevertiblePut;
import com.lbry.database.util.ArrayHelper;
import com.lbry.database.values.ActiveAmountValue;
import com.lbry.database.values.ClaimTakeoverValue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Slice;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
    public void tearDown(){}

    @Test
    public void testRollback() throws RocksDBException{
        String name = "derp";
        byte[] claim_hash1 = new byte[20];
        Arrays.fill(claim_hash1, (byte) 0x00);
        byte[] claim_hash2 = new byte[20];
        Arrays.fill(claim_hash2, (byte) 0x01);
        byte[] claim_hash3 = new byte[20];
        Arrays.fill(claim_hash3, (byte) 0x02);

        int takeoverHeight = 10000000;

        assertNull(this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }}));
        this.database.claim_takeover.stashPut(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claim_hash1;
            this.height = takeoverHeight;
        }});
        assertNull(this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }}));
        assertEquals(10000000,((ClaimTakeoverValue)this.database.claim_takeover.getPending(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }})).height);

        /////////////////////

        this.database.commit(10000000,ArrayHelper.fill(new byte[32],(byte) 0x00));
        assertEquals(10000000,((ClaimTakeoverValue)this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }})).height);

        /////////////////////

        this.database.claim_takeover.stashDelete(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claim_hash1;
            this.height = takeoverHeight;
        }});
        this.database.claim_takeover.stashPut(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claim_hash2;
            this.height = takeoverHeight + 1;
        }});
        this.database.claim_takeover.stashDelete(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claim_hash2;
            this.height = takeoverHeight + 1;
        }});
        this.database.commit(10000001,ArrayHelper.fill(new byte[32],(byte) 0x01));
        assertNull(this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }}));
        this.database.claim_takeover.stashPut(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claim_hash3;
            this.height = takeoverHeight + 2;
        }});
        this.database.commit(10000002,ArrayHelper.fill(new byte[32],(byte) 0x02));
        assertEquals(10000002,((ClaimTakeoverValue)this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }})).height);

        /////////////////////

        this.database.claim_takeover.stashDelete(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claim_hash3;
            this.height = takeoverHeight + 2;
        }});
        this.database.claim_takeover.stashPut(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }},new ClaimTakeoverValue(){{
            this.claim_hash = claim_hash2;
            this.height = takeoverHeight + 3;
        }});
        this.database.commit(10000003,ArrayHelper.fill(new byte[32],(byte) 0x03));
        assertEquals(10000003,((ClaimTakeoverValue)this.database.claim_takeover.get(new ClaimTakeoverKey(){{
            this.normalized_name = name;
        }})).height);

        /////////////////////

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
    public void testHubDatabaseIterator(){}

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