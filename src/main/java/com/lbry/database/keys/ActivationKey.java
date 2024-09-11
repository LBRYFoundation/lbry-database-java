package com.lbry.database.keys;

public class ActivationKey implements KeyInterface {

    public byte txo_type;
    public int tx_num;
    public short position;

    @Override
    public String toString() {
        return "ActivationKey{" +
                "txo_type=" + txo_type +
                ", tx_num=" + tx_num +
                ", position=" + position +
                '}';
    }

}
