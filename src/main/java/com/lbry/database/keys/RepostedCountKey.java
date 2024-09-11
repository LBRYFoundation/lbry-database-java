package com.lbry.database.keys;

import java.util.Arrays;

public class RepostedCountKey implements KeyInterface {

    public byte[] claim_hash;

    @Override
    public String toString() {
        return "RepostedCountKey{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                '}';
    }

}