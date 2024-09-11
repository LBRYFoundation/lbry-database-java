package com.lbry.database.values;

public class ActiveAmountValue implements ValueInterface {

    public long amount;

    @Override
    public String toString() {
        return "ActiveAmountValue{" +
                "amount=" + amount +
                '}';
    }

}