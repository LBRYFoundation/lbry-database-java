package com.lbry.database.values;

import java.util.Arrays;

public class ClaimToChannelValue implements ValueInterface {

    public byte[] signing_hash;

    @Override
    public String toString() {
        return "ClaimToChannelValue{" +
                "signing_hash=" + Arrays.toString(signing_hash) +
                '}';
    }

}