package com.lbry.database.values;

import java.util.List;

public class BlockTxsValue implements ValueInterface {

    public List<byte[]> tx_hashes;

    @Override
    public String toString() {
        return "BlockTxsValue{" +
                "tx_hashes=" + tx_hashes +
                '}';
    }

}