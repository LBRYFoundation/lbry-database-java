package com.lbry.database.keys;

import java.util.Arrays;

public class HashXUTXOKey implements KeyInterface {

    public byte[] short_tx_hash;
    public int tx_num;
    public short nout;

    @Override
    public String toString() {
        return "HashXUTXOKey{" +
                "short_tx_hash=" + Arrays.toString(short_tx_hash) +
                ", tx_num=" + tx_num +
                ", nout=" + nout +
                '}';
    }

}