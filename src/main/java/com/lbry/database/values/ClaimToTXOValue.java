package com.lbry.database.values;

public class ClaimToTXOValue implements ValueInterface {

    public int tx_num;
    public short position;
    public int root_tx_num;
    public short root_position;
    public long amount;
//    public int activation;
    public boolean channel_signature_is_valid;
    public String name;

    @Override
    public String toString() {
        return "ClaimToTXOValue{" +
                "tx_num=" + tx_num +
                ", position=" + position +
                ", root_tx_num=" + root_tx_num +
                ", root_position=" + root_position +
                ", amount=" + amount +
                ", channel_signature_is_valid=" + channel_signature_is_valid +
                ", name='" + name + '\'' +
                '}';
    }

}