package com.lbry.database.values;

import java.util.Set;

public class TouchedOrDeletedClaimValue implements ValueInterface {

    public Set<byte[]> touched_claims;
    public Set<byte[]> deleted_claims;

    @Override
    public String toString() {
        return "TouchedOrDeletedClaimValue{" +
                "touched_claims=" + touched_claims +
                ", deleted_claims=" + deleted_claims +
                '}';
    }

}
