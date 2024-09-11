package com.lbry.database.keys;

import java.util.Arrays;

public class HashXHistoryKey implements KeyInterface {

    public byte[] hashX;
    public int height;

    @Override
    public String toString() {
        return "HashXHistoryKey{" +
                "hashX=" + Arrays.toString(hashX) +
                ", height=" + height +
                '}';
    }

}
