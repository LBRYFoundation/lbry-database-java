package com.lbry.database.revert;

public class RevertiblePut extends RevertibleOperation{

    public RevertiblePut(byte[] key,byte[] value){
        super(key,value);
        this.isPut = true;
    }

    @Override
    public RevertibleOperation invert(){
        return new RevertibleDelete(this.key,this.value);
    }

}