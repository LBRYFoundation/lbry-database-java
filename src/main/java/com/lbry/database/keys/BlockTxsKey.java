package com.lbry.database.keys;

public class BlockTxsKey implements KeyInterface {

    public int height;

    @Override
    public String toString() {
        return "BlockTxsKey{" +
                "height=" + height +
                '}';
    }

}