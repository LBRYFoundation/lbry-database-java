package com.lbry.database.values;

import java.util.Arrays;

public class BlockHashValue implements ValueInterface {

    public byte[] block_hash;

    @Override
    public String toString() {
        return "BlockHashValue{" +
                "block_hash=" + Arrays.toString(block_hash) +
                '}';
    }

}