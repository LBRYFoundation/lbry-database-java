package com.lbry.database.values;

public class SupportAmountValue implements ValueInterface {

    public long amount;

    @Override
    public String toString() {
        return "SupportAmountValue{" +
                "amount=" + amount +
                '}';
    }

}
