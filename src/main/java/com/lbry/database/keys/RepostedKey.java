package com.lbry.database.keys;

import java.util.Arrays;

public class RepostedKey implements KeyInterface {

    public byte[] reposted_claim_hash;
    public int tx_num;
    public short position;

    @Override
    public String toString() {
        return "RepostedKey{" +
                "reposted_claim_hash=" + Arrays.toString(reposted_claim_hash) +
                ", tx_num=" + tx_num +
                ", position=" + position +
                '}';
    }

}