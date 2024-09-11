package com.lbry.database.values;

import java.util.Arrays;

public class ClaimTakeoverValue implements ValueInterface {

    public byte[] claim_hash;
    public int height;

    @Override
    public String toString() {
        return "ClaimTakeoverValue{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                ", height=" + height +
                '}';
    }

}