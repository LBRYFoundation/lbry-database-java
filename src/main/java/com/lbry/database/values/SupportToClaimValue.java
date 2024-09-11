package com.lbry.database.values;

import java.util.Arrays;

public class SupportToClaimValue implements ValueInterface {

    public byte[] claim_hash;

    @Override
    public String toString() {
        return "SupportToClaimValue{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                '}';
    }

}
