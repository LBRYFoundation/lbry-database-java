package com.lbry.database.values;

import java.util.List;

public class HashXHistoryValue implements ValueInterface {

    public List<Integer> tx_nums;

    @Override
    public String toString() {
        return "HashXHistoryValue{" +
                "tx_nums=" + tx_nums +
                '}';
    }

}
