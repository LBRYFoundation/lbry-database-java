package com.lbry.database.values;

import java.util.Arrays;

public class RepostValue implements ValueInterface {

    public byte[] reposted_claim_hash;

    @Override
    public String toString() {
        return "RepostValue{" +
                "reposted_claim_hash=" + Arrays.toString(reposted_claim_hash) +
                '}';
    }

}
