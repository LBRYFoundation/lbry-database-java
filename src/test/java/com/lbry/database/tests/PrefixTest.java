package com.lbry.database.tests;

import com.lbry.database.Prefix;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PrefixTest{

    @Test
    public void testDecodeEmptyJSON(){
        assertEquals('K',Prefix.CLAIM_TO_SUPPORT.getValue());
        assertEquals('L',Prefix.SUPPORT_TO_CLAIM.getValue());
    }

}