package com.lbry.database.values;

public class TxNumValue implements ValueInterface {

    public int tx_num;

    @Override
    public String toString() {
        return "TxNumValue{" +
                "tx_num=" + tx_num +
                '}';
    }

}
