package com.lbry.database.keys;

import java.util.Arrays;

public class ClaimToTXOKey implements KeyInterface {

    public byte[] claim_hash;

    @Override
    public String toString() {
        return "ClaimToTXOKey{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                '}';
    }

}