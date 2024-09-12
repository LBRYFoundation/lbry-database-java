package com.lbry.database.revert;

public class OperationStackIntegrityException extends RuntimeException{

    public OperationStackIntegrityException(String message){
        super(message);
    }

}