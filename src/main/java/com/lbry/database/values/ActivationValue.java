package com.lbry.database.values;

import java.util.Arrays;

public class ActivationValue implements ValueInterface {

    public int height;
    public byte[] claim_hash;
    public String normalized_name;

    @Override
    public String toString() {
        return "ActivationValue{" +
                "height=" + height +
                ", claim_hash=" + Arrays.toString(claim_hash) +
                ", normalized_name='" + normalized_name + '\'' +
                '}';
    }

}