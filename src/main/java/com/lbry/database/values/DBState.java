package com.lbry.database.values;

import java.util.Arrays;

public class DBState implements ValueInterface {

    public byte[] genesis;
    public int height;
    public int tx_count;
    public byte[] tip;
    public int utxo_flush_count;
    public int wall_time;
    public byte bit_fields;
    public byte db_version;
    public int hist_flush_count;
    public int comp_flush_count;
    public int comp_cursor;
    public int es_sync_height;
    public int hashX_status_last_indexed_height;

    @Override
    public String toString() {
        return "DBState{" +
                "genesis=" + Arrays.toString(genesis) +
                ", height=" + height +
                ", tx_count=" + tx_count +
                ", tip=" + Arrays.toString(tip) +
                ", utxo_flush_count=" + utxo_flush_count +
                ", wall_time=" + wall_time +
                ", bit_fields=" + bit_fields +
                ", db_version=" + db_version +
                ", hist_flush_count=" + hist_flush_count +
                ", comp_flush_count=" + comp_flush_count +
                ", comp_cursor=" + comp_cursor +
                ", es_sync_height=" + es_sync_height +
                ", hashX_status_last_indexed_height=" + hashX_status_last_indexed_height +
                '}';
    }

}