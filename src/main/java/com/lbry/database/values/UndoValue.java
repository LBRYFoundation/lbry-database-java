package com.lbry.database.values;

import java.util.Arrays;

public class UndoValue implements ValueInterface {

    public byte[] data;

    @Override
    public String toString() {
        return "UndoValue{" +
                "data=" + Arrays.toString(data) +
                '}';
    }

}
