package com.lbry.database.values;

import java.util.List;

public class TouchedHashXValue implements ValueInterface {

    public List<byte[]> touched_hashXs;

    @Override
    public String toString() {
        return "TouchedHashXValue{" +
                "touched_hashXs=" + touched_hashXs +
                '}';
    }

}
