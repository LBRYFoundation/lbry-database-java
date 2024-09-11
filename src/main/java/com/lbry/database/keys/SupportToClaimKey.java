package com.lbry.database.keys;

public class SupportToClaimKey implements KeyInterface {

    public int tx_num;
    public short position;

    @Override
    public String toString() {
        return "SupportToClaimKey{" +
                "tx_num=" + tx_num +
                ", position=" + position +
                '}';
    }

}
