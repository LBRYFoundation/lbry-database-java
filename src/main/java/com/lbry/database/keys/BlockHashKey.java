package com.lbry.database.keys;

public class BlockHashKey implements KeyInterface {

    public int height;

    @Override
    public String toString() {
        return "BlockHashKey{" +
                "height=" + height +
                '}';
    }

}