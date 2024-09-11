package com.lbry.database.values;

public class FutureEffectiveAmountValue implements ValueInterface {

    public long future_effective_amount;

    @Override
    public String toString() {
        return "FutureEffectiveAmountValue{" +
                "future_effective_amount=" + future_effective_amount +
                '}';
    }

}
