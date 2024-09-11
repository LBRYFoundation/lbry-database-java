package com.lbry.database.keys;

public class BidOrderKey implements KeyInterface {

    public String normalized_name;
    public long effective_amount;
    public int tx_num;
    public short position;

    @Override
    public String toString() {
        return "BidOrderKey{" +
                "normalized_name='" + normalized_name + '\'' +
                ", effective_amount=" + effective_amount +
                ", tx_num=" + tx_num +
                ", position=" + position +
                '}';
    }

}