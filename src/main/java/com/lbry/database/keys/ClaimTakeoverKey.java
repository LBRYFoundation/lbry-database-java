package com.lbry.database.keys;

public class ClaimTakeoverKey implements KeyInterface {

    public String normalized_name;

    @Override
    public String toString() {
        return "ClaimTakeoverKey{" +
                "normalized_name='" + normalized_name + '\'' +
                '}';
    }

}