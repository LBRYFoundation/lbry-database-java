package com.lbry.database.util;

public class Tuple2<A,B>{

    private final A a;
    private final B b;

    public Tuple2(A a,B b){
        this.a = a;
        this.b = b;
    }

    public A getA() {
        return this.a;
    }

    public B getB() {
        return this.b;
    }

}