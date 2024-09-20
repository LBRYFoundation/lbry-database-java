package com.lbry.database.util;

import java.util.Arrays;
import java.util.Map;

public class MapHelper{

    public static <V> byte[] getKey(Map<byte[],V> map,byte[] key){
        for(Map.Entry<byte[],V> entry : map.entrySet()){
            if(Arrays.equals(entry.getKey(),key)){
                return entry.getKey();
            }
        }
        return null;
    }

    public static <V> V getValue(Map<byte[],V> map,byte[] key){
        byte[] savedKey = MapHelper.getKey(map,key);
        if(savedKey!=null){
            return map.get(savedKey);
        }
        return null;
    }

    public static <V> V remove(Map<byte[],V> map,byte[] key){
        byte[] savedKey = MapHelper.getKey(map,key);
        if(savedKey!=null){
            return map.remove(savedKey);
        }
        return null;
    }

}