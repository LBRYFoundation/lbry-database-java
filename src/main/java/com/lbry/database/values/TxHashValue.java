package com.lbry.database.values;

import java.util.Arrays;

public class TxHashValue implements ValueInterface {

    public byte[] tx_hash;

    @Override
    public String toString() {
        return "TxHashValue{" +
                "tx_hash=" + Arrays.toString(tx_hash) +
                '}';
    }

}
