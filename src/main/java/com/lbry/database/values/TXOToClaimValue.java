package com.lbry.database.values;

import java.util.Arrays;

public class TXOToClaimValue implements ValueInterface {

    public byte[] claim_hash;
    public String name;

    @Override
    public String toString() {
        return "TXOToClaimValue{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                ", name='" + name + '\'' +
                '}';
    }

}
