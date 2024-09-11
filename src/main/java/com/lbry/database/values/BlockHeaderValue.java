package com.lbry.database.values;

import java.util.Arrays;

public class BlockHeaderValue implements ValueInterface {

    public byte[] header;

    @Override
    public String toString() {
        return "BlockHeaderValue{" +
                "header=" + Arrays.toString(header) +
                '}';
    }

}