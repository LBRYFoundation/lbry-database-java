package com.lbry.database.revert;

import com.lbry.database.util.Tuple2;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RevertibleOperationStack{

    private final Function<byte[],Optional<byte[]>> get;
    private final Function<List<byte[]>,Iterable<Optional<byte[]>>> multiGet;

    private final Map<byte[],RevertibleOperation[]> items;

    private final Deque<RevertibleOperation> stash;
    private final Map<byte[],RevertibleOperation> stashedLastOperationForKey;

    private final Set<Byte> unsafePrefixes;

    private final boolean enforceIntegrity;

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
            RevertibleOperation[] operationArr = null;
            for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                if(Arrays.equals(e.getKey(),operation.getKey())){
                    operationArr = e.getValue();
                }
            }
            if(operationArr!=null && operationArr.length>=1 && operation.invert().equals(operationArr[operationArr.length-1])){
                this.items.put(operationArr[0].getKey(),Arrays.copyOfRange(operationArr,0,operationArr.length-1));
                continue;
            }
            if(operationArr!=null && operationArr.length>=1 && operation.equals(operationArr[operationArr.length-1])){
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
                List<byte[]> batch = uniqueKeysList.subList(idx,idx+10000);
                Iterator<Optional<byte[]>> iterator = this.multiGet.apply(batch).iterator();
                for(byte[] k : batch){
                    byte[] v = iterator.next().get();
                    existing.put(k,v);
                }

            }
        }

        for(RevertibleOperation operation : needAppend){
            RevertibleOperation[] operationArr = null;
            for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                if(Arrays.equals(e.getKey(),operation.getKey())){
                    operationArr = e.getValue();
                }
            }

            if(operationArr!=null && operationArr.length>=1 && operationArr[operationArr.length-1].equals(operation)){
                this.items.put(operationArr[0].getKey(),Arrays.copyOfRange(operationArr,0,operationArr.length-1));
                RevertibleOperation[] operationArrX = null;
                for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                    if(Arrays.equals(e.getKey(),operation.getKey())){
                        operationArrX = e.getValue();
                    }
                }
                if(operationArrX==null || operationArrX.length==0){
                    this.items.remove(operation.getKey());
                }
            }
            if(!this.enforceIntegrity){
                RevertibleOperation[] operationArrX = null;
                for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                    if(Arrays.equals(e.getKey(),operation.getKey())){
                        operationArrX = e.getValue();
                    }
                }

                RevertibleOperation[] newArr = new RevertibleOperation[operationArrX==null?1:operationArrX.length+1];
                newArr[newArr.length-1] = operation;
                this.items.put(newArr[0].getKey(),newArr);
            }

            RevertibleOperation[] operationArrX = null;
            for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
                if(Arrays.equals(e.getKey(),operation.getKey())){
                    operationArrX = e.getValue();
                }
            }

            byte[] storedValue = existing.get(operation.getKey());
            boolean hasStoredValue = storedValue!=null;
            RevertibleOperation deleteStoredOperation = hasStoredValue?new RevertibleDelete(operation.getKey(),storedValue):null;
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

            RevertibleOperation[] newArr = new RevertibleOperation[operationArrX==null?1:operationArrX.length+1];
            newArr[newArr.length-1] = operation;
            this.items.put(newArr[0].getKey(),newArr);
        }

        this.stashedLastOperationForKey.clear();
    }

    public void appendOperation(RevertibleOperation operation){
        RevertibleOperation inverted = operation.invert();

        RevertibleOperation[] operationArr = null;
        for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
            if(Arrays.equals(e.getKey(),operation.getKey())){
                operationArr = e.getValue();
            }
        }
        if(operationArr!=null && operationArr.length>=1 && inverted.equals(operationArr[operationArr.length-1])){
            this.items.put(operationArr[0].getKey(),Arrays.copyOfRange(operationArr,0,operationArr.length-1));
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
        RevertibleOperation[] newArr = new RevertibleOperation[operationArrX==null?0:operationArrX.length];
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

    /**
     * Get the serialized bytes to undo all of the changes made by the pending ops
     */
    public byte[] getUndoOperations(){
        List<RevertibleOperation> reversed = new ArrayList<>();
        for(Map.Entry<byte[],RevertibleOperation[]> e : this.items.entrySet()){
            List<RevertibleOperation> operations = Arrays.asList(e.getValue());
            Collections.reverse(operations);
            reversed.addAll(operations);
        }
        List<byte[]> invertedAndPacked = new ArrayList<>();
        int size = 0;
        for(RevertibleOperation operation : reversed){
            byte[] undoOperation = operation.invert().pack();
            invertedAndPacked.add(undoOperation);
            size += undoOperation.length;
        }
        ByteBuffer bb = ByteBuffer.allocate(size);
        for(byte[] packed : invertedAndPacked){
            bb.put(packed);
        }
        return bb.array();
    }

    /**
     * Unpack and apply a sequence of undo ops from serialized undo bytes
     * @param packed
     */
    public void applyPackedUndoOperations(byte[] packed){
        while(packed.length>0){
            Tuple2<RevertibleOperation,byte[]> unpacked = RevertibleOperation.unpack(packed);
            this.appendOperation(unpacked.getA());
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