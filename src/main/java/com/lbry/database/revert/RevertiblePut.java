package com.lbry.database.revert;

public class RevertiblePut extends RevertibleOperation{

    protected boolean isPut = true;

    public RevertiblePut(byte[] key,byte[] value){
        super(key,value);
    }

    @Override
    public RevertibleOperation invert(){
        return new RevertibleDelete(this.key,this.value);
    }

}