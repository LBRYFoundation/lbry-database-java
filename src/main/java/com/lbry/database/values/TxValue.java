package com.lbry.database.values;

import java.util.Arrays;

public class TxValue implements ValueInterface {

    public byte[] raw_tx;

    @Override
    public String toString() {
        return "TxValue{" +
                "raw_tx=" + Arrays.toString(raw_tx) +
                '}';
    }

}
