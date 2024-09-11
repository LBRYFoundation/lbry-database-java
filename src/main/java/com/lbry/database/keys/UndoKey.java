package com.lbry.database.keys;

import java.util.Arrays;

public class UndoKey implements KeyInterface {

    public long height;
    public byte[] block_hash;

    @Override
    public String toString() {
        return "UndoKey{" +
                "height=" + height +
                ", block_hash=" + Arrays.toString(block_hash) +
                '}';
    }

}