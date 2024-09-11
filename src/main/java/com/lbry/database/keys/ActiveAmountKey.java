package com.lbry.database.keys;

import java.util.Arrays;

public class ActiveAmountKey implements KeyInterface {

    public byte[] claim_hash;
    public byte txo_type;
    public int activation_height;
    public int tx_num;
    public short position;

    @Override
    public String toString() {
        return "ActiveAmountKey{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                ", txo_type=" + txo_type +
                ", activation_height=" + activation_height +
                ", tx_num=" + tx_num +
                ", position=" + position +
                '}';
    }

}