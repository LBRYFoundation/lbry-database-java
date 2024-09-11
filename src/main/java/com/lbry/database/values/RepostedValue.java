package com.lbry.database.values;

import java.util.Arrays;

public class RepostedValue implements ValueInterface {

    public byte[] claim_hash;

    @Override
    public String toString() {
        return "RepostedValue{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                '}';
    }

}
