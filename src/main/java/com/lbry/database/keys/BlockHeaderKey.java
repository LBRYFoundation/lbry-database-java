package com.lbry.database.keys;

public class BlockHeaderKey implements KeyInterface {

    public int height;

    @Override
    public String toString() {
        return "BlockHeaderKey{" +
                "height=" + height +
                '}';
    }

}