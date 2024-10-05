package com.lbry.database.tests;

import com.lbry.database.keys.ClaimToTXOKey;
import com.lbry.database.revert.OperationStackIntegrityException;
import com.lbry.database.revert.RevertibleDelete;
import com.lbry.database.revert.RevertibleOperation;
import com.lbry.database.revert.RevertibleOperationStack;
import com.lbry.database.revert.RevertiblePut;
import com.lbry.database.rows.ClaimToTXOPrefixRow;
import com.lbry.database.util.MapHelper;
import com.lbry.database.values.ClaimToTXOValue;

import java.util.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RevertibleOperationStackTest {

    private Map<byte[],byte[]> fakeDatabase;
    private RevertibleOperationStack stack;

    @BeforeAll
    public void setUp(){
        class FakeDB extends HashMap<byte[],byte[]> implements Map<byte[],byte[]>{

            public Optional<byte[]> get2(byte[] key){
                for(Map.Entry<byte[],byte[]> e : this.entrySet()){
                    if(Arrays.equals(e.getKey(),key)){
                        return Optional.of(e.getValue());
                    }
                }
                return Optional.empty();
            }

            public Iterable<Optional<byte[]>> multiGet(List<byte[]> keys){
                List<Optional<byte[]>> values = new ArrayList<>();
                for(byte[] key : keys){
                    values.add(this.get2(key));
                }
                return values;
            }

        }

        this.fakeDatabase = new FakeDB();
        this.stack = new RevertibleOperationStack(((FakeDB)this.fakeDatabase)::get2,((FakeDB)this.fakeDatabase)::multiGet);
    }

    @AfterAll
    public void tearDown(){
        this.stack.clear();
        this.fakeDatabase.clear();
    }

    public void processStack(){
        System.err.println("PS: "+this.stack.iterate());
        for(RevertibleOperation operation : this.stack.iterate()){
            if(operation.isPut()){
                byte[] savedKey = MapHelper.getKey(this.fakeDatabase,operation.getKey());
                MapHelper.remove(this.fakeDatabase,savedKey);
                this.fakeDatabase.put(savedKey!=null?savedKey:operation.getKey(),operation.getValue());
            }else{
                MapHelper.remove(this.fakeDatabase,operation.getKey());
            }
        }
        this.stack.clear();
    }

    public void update(byte[] key1,byte[] value1,byte[] key2,byte[] value2){
//        System.err.println("UPD: DEL("+key1+" -> "+value1+") ==> PUT("+key2+" -> "+value2+")");
       // System.err.println("UPD: DEL("+new ClaimToTXOPrefixRow(null,null,null).unpackKey(key1)+" -> "+new ClaimToTXOPrefixRow(null,null,null).unpackValue(value1).name+") ==> PUT("+new ClaimToTXOPrefixRow(null,null,null).unpackKey(key2)+" -> "+new ClaimToTXOPrefixRow(null,null,null).unpackValue(value2).name+")");
        System.err.println("UPD: DEL(-> "+new ClaimToTXOPrefixRow(null,null,null).unpackValue(value1).name+") ==> PUT(-> "+new ClaimToTXOPrefixRow(null,null,null).unpackValue(value2).name+")");
//        System.err.println("INV: DEL("+key2+" -> "+value2+") ==> PUT("+key1+" -> "+value1+")");
        //System.err.println("INV: DEL("+new ClaimToTXOPrefixRow(null,null,null).unpackKey(key2)+" -> "+new ClaimToTXOPrefixRow(null,null,null).unpackValue(value2).name+") ==> PUT("+new ClaimToTXOPrefixRow(null,null,null).unpackKey(key1)+" -> "+new ClaimToTXOPrefixRow(null,null,null).unpackValue(value1).name+")");
//        System.err.println();
        this.stack.appendOperation(new RevertibleDelete(key1,value1));
        this.stack.appendOperation(new RevertiblePut(key2,value2));
    }

    @Test
    public void testSimplify(){
        ClaimToTXOKey k1 = new ClaimToTXOKey();
        k1.claim_hash = new byte[20];
        Arrays.fill(k1.claim_hash,(byte) 0x01);
        byte[] key1 = new ClaimToTXOPrefixRow(null,null,null).packKey(k1);
        ClaimToTXOKey k2 = new ClaimToTXOKey();
        k2.claim_hash = new byte[20];
        Arrays.fill(k2.claim_hash,(byte) 0x02);
        byte[] key2 = new ClaimToTXOPrefixRow(null,null,null).packKey(k2);
//        ClaimToTXOKey k3 = new ClaimToTXOKey();
//        k3.claim_hash = new byte[20];
//        Arrays.fill(k3.claim_hash,(byte) 0x03);
//        byte[] key3 = new ClaimToTXOPrefixRow(null,null).packKey(k3);
//        ClaimToTXOKey k4 = new ClaimToTXOKey();
//        k4.claim_hash = new byte[20];
//        Arrays.fill(k4.claim_hash,(byte) 0x04);
//        byte[] key4 = new ClaimToTXOPrefixRow(null,null).packKey(k4);

        ClaimToTXOValue v1 = new ClaimToTXOValue();
        v1.tx_num = 1;
        v1.position = 0;
        v1.root_tx_num = 1;
        v1.root_position = 0;
        v1.amount = 1;
        v1.channel_signature_is_valid = false;
        v1.name = "derp";
        byte[] val1 = new ClaimToTXOPrefixRow(null,null,null).packValue(v1);
        ClaimToTXOValue v2 = new ClaimToTXOValue();
        v2.tx_num = 1;
        v2.position = 0;
        v2.root_tx_num = 1;
        v2.root_position = 0;
        v2.amount = 1;
        v2.channel_signature_is_valid = false;
        v2.name = "oops";
        byte[] val2 = new ClaimToTXOPrefixRow(null,null,null).packValue(v2);
        ClaimToTXOValue v3 = new ClaimToTXOValue();
        v3.tx_num = 1;
        v3.position = 0;
        v3.root_tx_num = 1;
        v3.root_position = 0;
        v3.amount = 1;
        v3.channel_signature_is_valid = false;
        v3.name = "other";
        byte[] val3 = new ClaimToTXOPrefixRow(null,null,null).packValue(v3);

        // Check that we can't delete a non-existent value.
        assertThrows(OperationStackIntegrityException.class,() -> this.stack.appendOperation(new RevertibleDelete(key1,val1)));

        this.stack.appendOperation(new RevertiblePut(key1,val1));
        assertEquals(1,this.stack.length());
        this.stack.appendOperation(new RevertibleDelete(key1,val1));
        assertEquals(0,this.stack.length());

        this.stack.appendOperation(new RevertiblePut(key1,val1));
        assertEquals(1,this.stack.length());

        // Try to delete the wrong value.
        assertThrows(OperationStackIntegrityException.class,() -> this.stack.appendOperation(new RevertibleDelete(key2,val2)));

        this.stack.appendOperation(new RevertibleDelete(key1,val1));
        assertEquals(0,this.stack.length());
        this.stack.appendOperation(new RevertiblePut(key2,val3));
        assertEquals(1,this.stack.length());

        this.processStack();
        assertEquals(new HashMap<byte[],byte[]>(){{this.put(key2,val3);}},this.fakeDatabase);

        // Check that we can't put on top of the existing stored value.
        assertThrows(OperationStackIntegrityException.class,() -> this.stack.appendOperation(new RevertiblePut(key2,val1)));

        assertEquals(0,this.stack.length());
        this.stack.appendOperation(new RevertibleDelete(key2,val3));
        assertEquals(1,this.stack.length());
        this.stack.appendOperation(new RevertiblePut(key2,val3));
        assertEquals(0,this.stack.length());

        this.update(key2,val3,key2,val1);
        assertEquals(2,this.stack.length());

        this.processStack();
        assertEquals(new HashMap<byte[],byte[]>(){{this.put(key2,val1);}},this.fakeDatabase);

        this.update(key2,val1,key2,val2);
        assertEquals(2,this.stack.length());
        this.update(key2,val2,key2,val3);
        this.update(key2,val3,key2,val2);
        this.update(key2,val2,key2,val3);
        this.update(key2,val3,key2,val2);
        assertThrows(OperationStackIntegrityException.class,() -> this.update(key2,val3,key2,val2));

        this.update(key2,val2,key2,val3);
        assertEquals(2,this.stack.length());
        this.stack.appendOperation(new RevertibleDelete(key2,val3));
        this.processStack();
        this.processStack();
        assertEquals(new HashMap<>(),this.fakeDatabase);

        this.stack.appendOperation(new RevertiblePut(key2,val3));
        this.processStack();
        assertThrows(OperationStackIntegrityException.class,() -> this.update(key2,val2,key2,val2));

        this.update(key2,val3,key2,val2);
        assertEquals(new HashMap<byte[],byte[]>(){{this.put(key2,val3);}},this.fakeDatabase);
        byte[] undo = this.stack.getUndoOperations();
        this.processStack();
        assertEquals(new HashMap<byte[],byte[]>(){{this.put(key2,val2);}},this.fakeDatabase);


        System.err.println("VAL1: "+val1);
        System.err.println("VAL2: "+val2);
        System.err.println("VAL3: "+val3);

        System.err.println("BEFORE APPLY: "+this.fakeDatabase);
        System.err.println("BEFORE APPLY: "+this.stack.iterate());
        this.stack.applyPackedUndoOperations(undo);
        System.err.println("AFTER APPLY: "+this.fakeDatabase);
        System.err.println("AFTER APPLY: "+this.stack.iterate());
        this.processStack();
        System.err.println("FINAL: "+this.fakeDatabase);
        System.err.println("FINAL: "+this.stack.iterate());
        //assertEquals(this.fakeDatabase,new HashMap<byte[],byte[]>(){{this.put(key2,val2);}});//WRONG ONE
        //assertEquals(new HashMap<byte[],byte[]>(){{this.put(key2,val3);}},this.fakeDatabase);
    }

}