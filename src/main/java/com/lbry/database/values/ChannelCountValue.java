package com.lbry.database.values;

public class ChannelCountValue implements ValueInterface {

    public int count;

    @Override
    public String toString() {
        return "ChannelCountValue{" +
                "count=" + count +
                '}';
    }

}