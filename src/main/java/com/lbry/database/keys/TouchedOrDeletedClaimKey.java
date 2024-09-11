package com.lbry.database.keys;

public class TouchedOrDeletedClaimKey implements KeyInterface {

    public int height;

    @Override
    public String toString() {
        return "TouchedOrDeletedClaimKey{" +
                "height=" + height +
                '}';
    }

}