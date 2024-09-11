package com.lbry.database.keys;

import java.util.Arrays;

public class MempoolTxKey implements KeyInterface {

    public byte[] tx_hash;

    @Override
    public String toString() {
        return "MempoolTxKey{" +
                "tx_hash=" + Arrays.toString(tx_hash) +
                '}';
    }

}
