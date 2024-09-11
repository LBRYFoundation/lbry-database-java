package com.lbry.database.values;

import java.util.Arrays;

public class HashXUTXOValue implements ValueInterface {

    public byte[] hashX;

    @Override
    public String toString() {
        return "HashXUTXOValue{" +
                "hashX=" + Arrays.toString(hashX) +
                '}';
    }

}
