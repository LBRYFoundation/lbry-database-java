package com.lbry.database.values;

public class RepostedCountValue implements ValueInterface {

    public int reposted_count;

    @Override
    public String toString() {
        return "RepostedCountValue{" +
                "reposted_count=" + reposted_count +
                '}';
    }

}
