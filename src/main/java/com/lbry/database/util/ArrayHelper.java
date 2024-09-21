package com.lbry.database.util;

import com.lbry.database.revert.RevertibleOperation;

import java.lang.reflect.Array;
import java.util.Arrays;

public class ArrayHelper{

    public static RevertibleOperation[] append(RevertibleOperation[] arr,RevertibleOperation val){
        RevertibleOperation[] newArr = new RevertibleOperation[arr!=null?(arr.length+1):1];
        if(arr!=null){
            System.arraycopy(arr,0,newArr,0,arr.length);
        }
        newArr[newArr.length-1] = val;
        return newArr;
    }

    public static RevertibleOperation[] pop(RevertibleOperation[] arr){
        RevertibleOperation[] newArr = new RevertibleOperation[arr!=null?(arr.length==0?0:arr.length-1):0];
        if(arr!=null){
            System.arraycopy(arr,0,newArr,0,arr.length-1);
        }
        return newArr;
    }

    public static byte[] fill(byte[] arr,byte val){
        if(arr!=null){
            Arrays.fill(arr,val);
        }
        return arr;
    }

}