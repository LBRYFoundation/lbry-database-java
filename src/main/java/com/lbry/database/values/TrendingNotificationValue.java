package com.lbry.database.values;

public class TrendingNotificationValue implements ValueInterface {

    public long previous_amount;
    public long new_amount;

    @Override
    public String toString() {
        return "TrendingNotificationValue{" +
                "previous_amount=" + previous_amount +
                ", new_amount=" + new_amount +
                '}';
    }

}
