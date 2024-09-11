package com.lbry.database.keys;

public class TxHashKey implements KeyInterface {

    public int tx_num;

    @Override
    public String toString() {
        return "TxHashKey{" +
                "tx_num=" + tx_num +
                '}';
    }

}