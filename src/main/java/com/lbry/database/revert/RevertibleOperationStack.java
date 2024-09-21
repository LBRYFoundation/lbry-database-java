package com.lbry.database.revert;

import com.lbry.database.util.ArrayHelper;
import com.lbry.database.util.MapHelper;
import com.lbry.database.util.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RevertibleOperationStack{

    private final Function<byte[],Optional<byte[]>> get;
    private final Function<List<byte[]>,Iterable<Optional<byte[]>>> multiGet;

    private final Map<byte[],RevertibleOperation[]> items;

    private final Deque<RevertibleOperation> stash;
    private final Map<byte[],RevertibleOperation> stashedLastOperationForKey;

    private final Set<Byte> unsafePrefixes;

    private final boolean enforceIntegrity;

    public RevertibleOperationStack(Function<byte[],Optional<byte[]>> get,Function<List<byte[]>,Iterable<Optional<byte[]>>> multiGet){
        this(get,multiGet,null);
    }

    public RevertibleOperationStack(Function<byte[],Optional<byte[]>> get,Function<List<byte[]>,Iterable<Optional<byte[]>>> multiGet,Set<Byte> unsafePrefixes){
        this(get,multiGet,unsafePrefixes,true);
    }

    public RevertibleOperationStack(Function<byte[],Optional<byte[]>> get,Function<List<byte[]>,Iterable<Optional<byte[]>>> multiGet,Set<Byte> unsafePrefixes,boolean enforceIntegrity){
        this.get = get;
        this.multiGet = multiGet;

        this.items = new HashMap<>();

        this.stash = new ArrayDeque<>();
        this.stashedLastOperationForKey = new HashMap<>();

        this.unsafePrefixes = unsafePrefixes!=null?unsafePrefixes:new HashSet<>();
        this.enforceIntegrity = enforceIntegrity;
    }

    public void stashOperations(RevertibleOperation[] operations){
        this.stash.addAll(Arrays.asList(operations));
        for(RevertibleOperation operation : operations){
            this.stashedLastOperationForKey.put(operation.key,operation);
        }
    }

    public void validateAndApplyStashedOperations(){
        if(this.stash.isEmpty()){
            return;
        }

        List<RevertibleOperation> needAppend = new ArrayList<>();
        Set<byte[]> uniqueKeys = new HashSet<>();

        while(!this.stash.isEmpty()){
            RevertibleOperation operation = this.stash.pollFirst();

            RevertibleOperation[] operationArr = MapHelper.getValue(this.items,operation.key);
            if(operationArr!=null && operationArr.length>=1 && operation.invert().equals(operationArr[operationArr.length-1])){
                this.items.replace(operationArr[0].getKey(),Arrays.copyOfRange(operationArr,0,operationArr.length-1));
                continue;
            }else if(operationArr!=null && operationArr.length>=1 && operation.equals(operationArr[operationArr.length-1])){
                continue;
            }else{
                needAppend.add(operation);
                uniqueKeys.add(operation.getKey());
            }
        }

        Map<byte[],byte[]> existing = new HashMap<>();
        if(this.enforceIntegrity && !uniqueKeys.isEmpty()){
            List<byte[]> uniqueKeysList = new ArrayList<>(uniqueKeys);
            for(int idx=0;idx<uniqueKeys.size();idx+=10000){
                List<byte[]> batch = uniqueKeysList.subList(idx,Math.min(uniqueKeysList.size(),idx+10000));
                Iterator<Optional<byte[]>> iterator = this.multiGet.apply(batch).iterator();
                for(byte[] k : batch){
                    Optional<byte[]> vOpt = iterator.next();
                    vOpt.ifPresent(bytes -> existing.put(k, bytes));
                }

            }
        }

        for(RevertibleOperation operation : needAppend){
            RevertibleOperation[] operationArr = MapHelper.getValue(this.items,operation.getKey());
            if(operationArr!=null && operationArr.length>=1 && operationArr[operationArr.length-1].equals(operation)){
                this.items.put(MapHelper.getKey(this.items,operation.getKey()),ArrayHelper.pop(MapHelper.getValue(this.items,operation.getKey())));
                RevertibleOperation[] operationArr2 = MapHelper.getValue(this.items,operation.getKey());
                if(operationArr2==null || operationArr2.length==0){
                    MapHelper.remove(this.items,operation.getKey());
                }
            }
            if(!this.enforceIntegrity){
                byte[] newKey = MapHelper.getKey(this.items,operation.getKey());
                this.items.put(newKey,ArrayHelper.append(MapHelper.getValue(this.items,newKey),operation));
                continue;
            }
            byte[] storedValue = MapHelper.getValue(existing,operation.getKey());
            boolean hasStoredValue = storedValue!=null;
            RevertibleOperation deleteStoredOperation = !hasStoredValue?null:new RevertibleDelete(operation.getKey(),storedValue);
            boolean willDeleteExistingRecord = deleteStoredOperation!=null && (MapHelper.getValue(this.items,operation.getKey())!=null);
            try{
                if(operation.isDelete()){
                    if(hasStoredValue && !Arrays.equals(storedValue,operation.value) && !willDeleteExistingRecord){
                        // There is a value and we're not deleting it in this operation.
                        // Check that a delete for the stored value is in the stack.
                        throw new OperationStackIntegrityException("Database operation tries to delete with incorrect existing value "+operation+"\nvs\n"+new String(storedValue));
                    }else if(!hasStoredValue){
                        throw new OperationStackIntegrityException("Database operation tries to delete nonexistent key: "+operation);
                    }else if(!Arrays.equals(storedValue,operation.value)){
                        throw new OperationStackIntegrityException("Database operation tries to delete with incorrect value: "+operation);
                    }
                }else{
                    if(hasStoredValue && !willDeleteExistingRecord){
                        throw new OperationStackIntegrityException("Database operation tries to overwrite before deleting existing: "+operation);
                    }
                    RevertibleOperation[] operationArrY = null;
                    for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                        if(Arrays.equals(e.getKey(),operation.getKey())){
                            operationArrY = e.getValue();
                        }
                    }
                    if(operationArrY!=null && operationArrY.length>=1 && operationArrY[operationArrY.length-1].isPut){
                        throw new OperationStackIntegrityException("Database operation tries to overwrite with "+operation+" before deleting pending: "+operationArrY[operationArrY.length-1]);
                    }
                }
            }catch(OperationStackIntegrityException e){
                if(this.unsafePrefixes.contains(operation.getKey()[0])){
                    System.err.println("Skipping over integrity error: "+e);
                }else{
                    throw e;
                }
            }
            byte[] newKey = MapHelper.getKey(this.items,operation.getKey());
            this.items.put(newKey,ArrayHelper.append(MapHelper.getValue(this.items,newKey),operation));
        }

        this.stashedLastOperationForKey.clear();
    }

    /**
     * Apply a put or delete op, checking that it introduces no integrity errors.
     * @param operation The revertible operation
     */
    public void appendOperation(RevertibleOperation operation){
        RevertibleOperation inverted = operation.invert();

        RevertibleOperation[] operationArr = MapHelper.getValue(this.items,operation.getKey());
        if(operationArr!=null && operationArr.length>=1 && inverted.equals(operationArr[operationArr.length-1])){
            // If the new op is the inverse of the last op, we can safely null both.
            this.items.put(operationArr[0].getKey(),Arrays.copyOfRange(operationArr,0,operationArr.length-1));
            return;
        }else if(operationArr!=null && operationArr.length>=1 && operationArr[operationArr.length-1].equals(operation)){
            // Duplicate of last operation.
            return; // Raise an error?
        }

        Optional<byte[]> storedValue = this.get.apply(operation.getKey());
        boolean hasStoredValue = storedValue.isPresent();
        RevertibleOperation deleteStoredOperation = hasStoredValue?new RevertibleDelete(operation.getKey(),storedValue.get()):null;
        boolean deleteStoredOperationInOperationList = false;
        if(operationArr!=null){
            for(RevertibleOperation o : operationArr){
                if(o.equals(deleteStoredOperation)){
                    deleteStoredOperationInOperationList = true;
                    break;
                }
            }
        }
        boolean willDeleteExistingRecord = deleteStoredOperation!=null && deleteStoredOperationInOperationList;

        try{
            if(operation.isPut && hasStoredValue && !willDeleteExistingRecord){
                throw new OperationStackIntegrityException("Database operation tries to add on top of existing key without deleting first: "+operation);
            }else if(operation.isDelete() && hasStoredValue && !Arrays.equals(storedValue.get(),operation.getValue()) && !willDeleteExistingRecord){
                // There is a value and we're not deleting it in this operation.
                // Check that a delete for the stored value is in the stack.
                throw new OperationStackIntegrityException("Database operation tries to delete with incorrect existing value "+operation);
            }else if(operation.isDelete() && !hasStoredValue){
                throw new OperationStackIntegrityException("Database operation tries to delete nonexistent key: "+operation);
            }else if(operation.isDelete() && !Arrays.equals(storedValue.get(),operation.getValue())){
                throw new OperationStackIntegrityException("Database operation tries to delete with incorrect value: "+operation);
            }
        }catch(OperationStackIntegrityException e){
            if(this.unsafePrefixes.contains(operation.getKey()[0])){
                System.err.println("Skipping over integrity error: "+e);
            }else{
                throw e;
            }
        }

        RevertibleOperation[] operationArrX = null;
        for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
            if(Arrays.equals(e.getKey(),operation.getKey())){
                operationArrX = e.getValue();
            }
        }
        RevertibleOperation[] newArr = new RevertibleOperation[operationArrX==null?1:operationArrX.length+1];
        if(operationArrX!=null){
            System.arraycopy(operationArrX,0,newArr,0,operationArrX.length);
        }
        newArr[newArr.length-1] = operation;
        this.items.put(newArr[0].getKey(),newArr);
    }

    /**
     * Apply a put or delete op, checking that it introduces no integrity errors
     * @param operations
     */
    public void multiPut(List<RevertiblePut> operations){
        if(operations==null){
            return;
        }
        for(RevertibleOperation op : operations){
            if(!op.isPut){
                throw new RuntimeException("List must contain only put operations.");
            }
        }
        Map<byte[],RevertibleOperation> keys = new HashMap<>();
        for(RevertibleOperation operation : operations){
            keys.put(operation.getKey(),operation);
        }
        if(keys.keySet().size()!=operations.size()){
            throw new RuntimeException("List must contain unique keys.");
        }

        List<RevertibleOperation> needPut = new ArrayList<>();
        for(RevertibleOperation operation : operations){
            RevertibleOperation[] operationArr = null;
            for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                if(Arrays.equals(e.getKey(),operation.getKey())){
                    operationArr = e.getValue();
                }
            }
            if(operationArr!=null && operationArr.length>=1 && operation.invert().equals(operationArr[operationArr.length-1])){
                this.items.put(operationArr[0].getKey(),Arrays.copyOfRange(operationArr,0,operationArr.length-1));
                continue;
            }else if(operationArr!=null && operationArr.length>=1 && operation.equals(operationArr[operationArr.length-1])){
                continue; // Raise an error?
            }else{
                needPut.add(operation);
            }
        }

        Iterator<Optional<byte[]>> storedValues = this.multiGet.apply(needPut.stream().map(RevertibleOperation::getKey).collect(Collectors.toList())).iterator();
        for(RevertibleOperation operation : needPut){
            Optional<byte[]> storedValue = storedValues.next();

            boolean hasStoredValue = storedValue.isPresent();
            RevertibleOperation deleteStoredOperation = hasStoredValue?new RevertibleDelete(operation.getKey(),storedValue.get()):null;
            RevertibleOperation[] operationArrX = null;
            for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                if(Arrays.equals(e.getKey(),operation.getKey())){
                    operationArrX = e.getValue();
                }
            }
            boolean deleteStoredOperationInOperationList = false;
            if(operationArrX!=null){
                for(RevertibleOperation o : operationArrX){
                    if(o.equals(deleteStoredOperation)){
                        deleteStoredOperationInOperationList = true;
                        break;
                    }
                }
            }
            boolean willDeleteExistingRecord = deleteStoredOperation!=null && deleteStoredOperationInOperationList;

            try{
                if(hasStoredValue && !willDeleteExistingRecord){
                    throw new OperationStackIntegrityException("Database operation tries to overwrite before deleting existing: "+operation);
                }
            }catch(OperationStackIntegrityException e){
                if(this.unsafePrefixes.contains(operation.getKey()[0])){
                    System.err.println("Skipping over integrity error: "+e);
                }else{
                    throw e;
                }
            }

            RevertibleOperation[] operationArr = null;
            for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                if(Arrays.equals(e.getKey(),operation.getKey())){
                    operationArr = e.getValue();
                }
            }
            RevertibleOperation[] newArr = new RevertibleOperation[operationArr==null?1:operationArr.length+1];
            newArr[newArr.length-1] = operation;
            this.items.put(newArr[0].getKey(),newArr);
        }
    }

    /**
     * Apply a put or delete op, checking that it introduces no integrity errors
     * @param operations
     */
    public void multiDelete(List<RevertibleDelete> operations){
        if(operations==null){
            return;
        }
        for(RevertibleOperation op : operations){
            if(op.isDelete()){
                throw new RuntimeException("List must contain only delete operations.");
            }
        }
        Map<byte[],RevertibleOperation> keys = new HashMap<>();
        for(RevertibleOperation operation : operations){
            keys.put(operation.getKey(),operation);
        }
        if(keys.keySet().size()!=operations.size()){
            throw new RuntimeException("List must contain unique keys.");
        }

        List<RevertibleOperation> needDelete = new ArrayList<>();
        for(RevertibleOperation operation : operations){
            RevertibleOperation[] operationArr = null;
            for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                if(Arrays.equals(e.getKey(),operation.getKey())){
                    operationArr = e.getValue();
                }
            }
            if(operationArr!=null && operationArr.length>=1 && operation.invert().equals(operationArr[operationArr.length-1])){
                this.items.put(operationArr[0].getKey(),Arrays.copyOfRange(operationArr,0,operationArr.length-1));
                continue;
            }else if(operationArr!=null && operationArr.length>=1 && operation.equals(operationArr[operationArr.length-1])){
                continue; // Raise an error?
            }else{
                needDelete.add(operation);
            }
        }

        Iterator<Optional<byte[]>> storedValues = this.multiGet.apply(needDelete.stream().map(RevertibleOperation::getKey).collect(Collectors.toList())).iterator();
        for(RevertibleOperation operation : needDelete){
            Optional<byte[]> storedValue = storedValues.next();

            boolean hasStoredValue = storedValue.isPresent();
            RevertibleOperation deleteStoredOperation = hasStoredValue?new RevertibleDelete(operation.getKey(),storedValue.get()):null;
            RevertibleOperation[] operationArrX = null;
            for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                if(Arrays.equals(e.getKey(),operation.getKey())){
                    operationArrX = e.getValue();
                }
            }
            boolean deleteStoredOperationInOperationList = false;
            if(operationArrX!=null){
                for(RevertibleOperation o : operationArrX){
                    if(o.equals(deleteStoredOperation)){
                        deleteStoredOperationInOperationList = true;
                        break;
                    }
                }
            }
            boolean willDeleteExistingRecord = deleteStoredOperation!=null && deleteStoredOperationInOperationList;

            try{
                if(operation.isDelete() && hasStoredValue && Arrays.equals(storedValue.get(),operation.getValue()) && !willDeleteExistingRecord){
                    // There is a value and we're not deleting it in this operation.
                    // Check that a delete for the stored value is in the stack.
                    throw new OperationStackIntegrityException("Database operation tries to delete with incorrect existing value "+operation);
                }else if(!storedValue.isPresent()){
                    throw new OperationStackIntegrityException("Database operation tries to delete nonexistent key: "+operation);
                }else if(operation.isDelete() && Arrays.equals(storedValue.get(),operation.getValue())){
                    throw new OperationStackIntegrityException("Database operation tries to delete with incorrect value: "+operation);
                }
            }catch(OperationStackIntegrityException e){
                if(this.unsafePrefixes.contains(operation.getKey()[0])){
                    System.err.println("Skipping over integrity error: "+e);
                }else{
                    throw e;
                }
            }

            RevertibleOperation[] operationArr = null;
            for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                if(Arrays.equals(e.getKey(),operation.getKey())){
                    operationArr = e.getValue();
                }
            }
            RevertibleOperation[] newArr = new RevertibleOperation[operationArr==null?1:operationArr.length+1];
            newArr[newArr.length-1] = operation;
            this.items.put(newArr[0].getKey(),newArr);
        }
    }

    public void clear(){
        this.items.clear();
        this.stash.clear();
        this.stashedLastOperationForKey.clear();
    }

    public int length(){
        return this.items.values().stream().mapToInt(x -> x.length).sum();
    }

    public Iterable<RevertibleOperation> iterate(){
        return this.items.values().stream().flatMap(Stream::of).collect(Collectors.toList());
    }

    /**
     * Get the serialized bytes to undo all of the changes made by the pending ops
     */
    public byte[] getUndoOperations(){
        List<RevertibleOperation> reversed = new ArrayList<>();
        for(RevertibleOperation operation : this.iterate()){
            reversed.add(operation);
        }
        Collections.reverse(reversed);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        for(RevertibleOperation operation : reversed){
            try{
                baos.write(operation.invert().pack());
            }catch(IOException e){
                e.printStackTrace();
            }
        }
        return baos.toByteArray();
    }

    /**
     * Unpack and apply a sequence of undo ops from serialized undo bytes
     * @param packed
     */
    public void applyPackedUndoOperations(byte[] packed){
        while(packed.length>0){
            Tuple2<RevertibleOperation,byte[]> unpacked = RevertibleOperation.unpack(packed);
            this.stash.add(unpacked.getA());
            byte[] savedKey = MapHelper.getKey(this.stashedLastOperationForKey,unpacked.getA().getKey());
            this.stashedLastOperationForKey.put(savedKey!=null?savedKey:unpacked.getA().getKey(),unpacked.getA());
            packed = unpacked.getB();
        }
    }

    public Optional<RevertibleOperation> getPendingOperation(byte[] key){
        for(Map.Entry<byte[],RevertibleOperation> e : this.stashedLastOperationForKey.entrySet()){
            if(Arrays.equals(e.getKey(),key)){
                return Optional.of(e.getValue());
            }
        }
        for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
            if(Arrays.equals(e.getKey(),key)){
                if(e.getValue().length>=1){
                    return Optional.of(e.getValue()[e.getValue().length-1]);
                }
            }
        }
        return Optional.empty();
    }

}