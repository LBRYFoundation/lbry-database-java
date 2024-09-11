package com.lbry.database.revert;

public class RevertibleDelete extends RevertibleOperation{

    public RevertibleDelete(byte[] key,byte[] value){
        super(key,value);
    }

    @Override
    public RevertibleOperation invert(){
        return new RevertiblePut(this.key,this.value);
    }

}
