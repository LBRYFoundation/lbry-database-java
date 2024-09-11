package com.lbry.database.values;

import java.util.Arrays;

public class MempoolTxValue implements ValueInterface {

    public byte[] raw_tx;

    @Override
    public String toString() {
        return "MempoolTxValue{" +
                "raw_tx=" + Arrays.toString(raw_tx) +
                '}';
    }

}
