package com.lbry.database.keys;

public class PendingActivationKey implements KeyInterface {

    public int height;
    public byte txo_type;
    public int tx_num;
    public short position;

    @Override
    public String toString() {
        return "PendingActivationKey{" +
                "height=" + height +
                ", txo_type=" + txo_type +
                ", tx_num=" + tx_num +
                ", position=" + position +
                '}';
    }

}