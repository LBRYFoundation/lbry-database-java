package com.lbry.database.keys;

public class TXOToClaimKey implements KeyInterface {

    public int tx_num;
    public short position;

    @Override
    public String toString() {
        return "TXOToClaimKey{" +
                "tx_num=" + tx_num +
                ", position=" + position +
                '}';
    }

}