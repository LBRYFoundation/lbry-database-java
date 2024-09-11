package com.lbry.database.values;

public class EffectiveAmountValue implements ValueInterface {

    public long activated_sum;
    public long activated_support_sum;

    @Override
    public String toString() {
        return "EffectiveAmountValue{" +
                "activated_sum=" + activated_sum +
                ", activated_support_sum=" + activated_support_sum +
                '}';
    }

}
