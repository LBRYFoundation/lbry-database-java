package com.lbry.database.keys;

import java.util.Arrays;

public class TrendingNotificationKey implements KeyInterface {

    public int height;
    public byte[] claim_hash;

    @Override
    public String toString() {
        return "TrendingNotificationKey{" +
                "height=" + height +
                ", claim_hash=" + Arrays.toString(claim_hash) +
                '}';
    }

}