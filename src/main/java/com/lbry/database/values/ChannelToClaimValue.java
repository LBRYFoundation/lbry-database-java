package com.lbry.database.values;

import java.util.Arrays;

public class ChannelToClaimValue implements ValueInterface {

    public byte[] claim_hash;

    @Override
    public String toString() {
        return "ChannelToClaimValue{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                '}';
    }

}