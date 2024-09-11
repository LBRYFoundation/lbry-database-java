package com.lbry.database.keys;

public class TxCountKey implements KeyInterface {

    public int height;

    @Override
    public String toString() {
        return "TxCountKey{" +
                "height=" + height +
                '}';
    }

}