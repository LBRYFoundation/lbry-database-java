package com.lbry.database.keys;

public class ClaimShortIDKey implements KeyInterface {

    public String normalized_name;
    public String partial_claim_id;
    public int root_tx_num;
    public short root_position;

    @Override
    public String toString() {
        return "ClaimShortIDKey{" +
                "normalized_name='" + normalized_name + '\'' +
                ", partial_claim_id='" + partial_claim_id + '\'' +
                ", root_tx_num=" + root_tx_num +
                ", root_position=" + root_position +
                '}';
    }

}