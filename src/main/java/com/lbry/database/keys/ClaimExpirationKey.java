package com.lbry.database.keys;

public class ClaimExpirationKey implements KeyInterface {

    public int expiration;
    public int tx_num;
    public short position;

    @Override
    public String toString() {
        return "ClaimExpirationKey{" +
                "expiration=" + expiration +
                ", tx_num=" + tx_num +
                ", position=" + position +
                '}';
    }

}