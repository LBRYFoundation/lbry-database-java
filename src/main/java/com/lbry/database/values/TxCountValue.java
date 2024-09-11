package com.lbry.database.values;

public class TxCountValue implements ValueInterface {

    public int tx_count;

    @Override
    public String toString() {
        return "TxCountValue{" +
                "tx_count=" + tx_count +
                '}';
    }

}
