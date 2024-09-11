package com.lbry.database.keys;

import java.util.Arrays;

public class TxKey implements KeyInterface {

    public byte[] tx_hash;

    @Override
    public String toString() {
        return "TxKey{" +
                "tx_hash=" + Arrays.toString(tx_hash) +
                '}';
    }

}