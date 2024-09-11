package com.lbry.database.keys;

import java.util.Arrays;

public class ClaimToSupportKey implements KeyInterface {

    public byte[] claim_hash;
    public int tx_hash;
    public short position;

    @Override
    public String toString() {
        return "ClaimToSupportKey{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                ", tx_hash=" + tx_hash +
                ", position=" + position +
                '}';
    }

}
