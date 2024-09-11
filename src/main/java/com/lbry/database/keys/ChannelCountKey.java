package com.lbry.database.keys;

import java.util.Arrays;

public class ChannelCountKey implements KeyInterface {

    public byte[] channel_hash;

    @Override
    public String toString() {
        return "ChannelCountKey{" +
                "channel_hash=" + Arrays.toString(channel_hash) +
                '}';
    }

}