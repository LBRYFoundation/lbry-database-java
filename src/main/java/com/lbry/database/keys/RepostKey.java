package com.lbry.database.keys;

import java.util.Arrays;

public class RepostKey implements KeyInterface {

    public byte[] claim_hash;

    @Override
    public String toString() {
        return "RepostKey{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                '}';
    }

}