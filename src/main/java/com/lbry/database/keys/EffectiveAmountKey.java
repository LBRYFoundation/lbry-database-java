package com.lbry.database.keys;

import java.util.Arrays;

public class EffectiveAmountKey implements KeyInterface {

    public byte[] claim_hash;

    @Override
    public String toString() {
        return "EffectiveAmountKey{" +
                "claim_hash=" + Arrays.toString(claim_hash) +
                '}';
    }
    
}