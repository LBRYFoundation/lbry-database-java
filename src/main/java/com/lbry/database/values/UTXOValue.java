package com.lbry.database.values;

public class UTXOValue implements ValueInterface {

    public long amount;

    @Override
    public String toString() {
        return "UTXOValue{" +
                "amount=" + amount +
                '}';
    }

}
