package com.lbry.database.keys;

import java.util.Arrays;

public class ClaimToChannelKey implements KeyInterface {

    public byte[] claim_hash;
    public int tx_num;
    public short position;

    @Override
    public String toString() {
        return "ClaimToChannelKey{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                ", tx_num=" + tx_num +
                ", position=" + position +
                '}';
    }

}
