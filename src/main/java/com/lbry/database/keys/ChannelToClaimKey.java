package com.lbry.database.keys;

import java.util.Arrays;

public class ChannelToClaimKey implements KeyInterface {

    public byte[] signing_hash;
    public String name;
    public int tx_num;
    public short position;

    @Override
    public String toString() {
        return "ChannelToClaimKey{" +
                "signing_hash=" + Arrays.toString(signing_hash) +
                ", name='" + name + '\'' +
                ", tx_num=" + tx_num +
                ", position=" + position +
                '}';
    }

}
