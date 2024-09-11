package com.lbry.database.keys;

import java.util.Arrays;

public class UTXOKey implements KeyInterface {

    public byte[] hashX;
    public int tx_num;
    public short nout;

    @Override
    public String toString() {
        return "UTXOKey{" +
                "hashX=" + Arrays.toString(hashX) +
                ", tx_num=" + tx_num +
                ", nout=" + nout +
                '}';
    }

}