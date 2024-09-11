package com.lbry.database.values;

import java.util.Arrays;

public class HashXStatusValue implements ValueInterface {

    public byte[] status;

    @Override
    public String toString() {
        return "HashXStatusValue{" +
                "status=" + Arrays.toString(status) +
                '}';
    }

}