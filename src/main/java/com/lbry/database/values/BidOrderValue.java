package com.lbry.database.values;

import java.util.Arrays;

public class BidOrderValue implements ValueInterface {

    public byte[] claim_hash;

    @Override
    public String toString() {
        return "BidOrderValue{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                '}';
    }

}