package com.lbry.database.values;

public class ClaimShortIDValue implements ValueInterface {

    public int tx_num;
    public short position;

    @Override
    public String toString() {
        return "ClaimShortIDValue{" +
                "tx_num=" + tx_num +
                ", position=" + position +
                '}';
    }

}