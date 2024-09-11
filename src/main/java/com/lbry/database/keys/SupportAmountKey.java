package com.lbry.database.keys;

import java.util.Arrays;

public class SupportAmountKey implements KeyInterface {

    public byte[] claim_hash;

    @Override
    public String toString() {
        return "SupportAmountKey{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                '}';
    }

}