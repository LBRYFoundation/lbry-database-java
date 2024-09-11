package com.lbry.database.values;

import java.util.Arrays;

public class ClaimExpirationValue implements ValueInterface {

    public byte[] claim_hash;
    public String normalized_name;

    @Override
    public String toString() {
        return "ClaimExpirationValue{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                ", normalized_name='" + normalized_name + '\'' +
                '}';
    }

}