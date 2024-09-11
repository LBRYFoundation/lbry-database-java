package com.lbry.database.revert;

import java.util.Arrays;

public abstract class RevertibleOperation{

    protected byte[] key;
    protected byte[] value;

    protected boolean isPut;

    public RevertibleOperation(byte[] key,byte[] value){
        this.key = key;
        this.value = value;
    }

    public byte[] getKey(){
        return this.key;
    }

    public byte[] getValue(){
        return this.value;
    }

    public boolean isDelete(){
        return !this.isPut;
    }

    public RevertibleOperation invert(){
        throw new RuntimeException("Not implemented");
    }

    //TODO PACK
    //TODO UNPACK


    @Override
    public boolean equals(Object obj){
        if(obj instanceof RevertibleOperation){
            RevertibleOperation op = (RevertibleOperation) obj;
            return this.isPut==op.isPut && Arrays.equals(this.key,op.key) && Arrays.equals(this.value,op.value);
        }
        return false;
    }

    //TODO REPR
    //TODO STR

}