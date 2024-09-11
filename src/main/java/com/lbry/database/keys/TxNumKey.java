package com.lbry.database.keys;

import java.util.Arrays;

public class TxNumKey implements KeyInterface {

    public byte[] tx_hash;

    @Override
    public String toString() {
        return "TxNumKey{" +
                "tx_hash=" + Arrays.toString(tx_hash) +
                '}';
    }

}