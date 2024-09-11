package com.lbry.database.values;

public class ClaimToSupportValue implements ValueInterface {

    public long amount;

    @Override
    public String toString() {
        return "ClaimToSupportValue{" +
                "amount=" + amount +
                '}';
    }

}
