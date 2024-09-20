package com.lbry.database.tests;

import com.lbry.database.PrefixDB;
import com.lbry.database.keys.ClaimTakeoverKey;
import com.lbry.database.util.ArrayHelper;
import com.lbry.database.values.ClaimTakeoverValue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.rocksdb.RocksDBException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RevertablePrefixDBTest{

    private File tmpDir;
    private PrefixDB database;

    @BeforeAll
    public void setUp() throws IOException,RocksDBException{
        this.tmpDir = Files.createTempDirectory("tmp").toFile();
        System.err.println(this.tmpDir);
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
    public void testHubDatabaseIteratorStartStop(){}

}