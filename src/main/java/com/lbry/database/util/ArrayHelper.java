package com.lbry.database.util;

import java.util.Arrays;

public class ArrayHelper{

    public static byte[] fill(byte[] arr,byte val){
        if(arr!=null){
            Arrays.fill(arr,val);
        }
        return arr;
    }

}