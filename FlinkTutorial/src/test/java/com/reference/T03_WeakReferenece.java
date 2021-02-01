package com.reference;

import java.lang.ref.WeakReference;

public class T03_WeakReferenece {
    public static void main(String[] args) {
        WeakReference<M> wr = new WeakReference<>(new M());

        System.out.println(wr.get());
        System.gc();
        System.out.println(wr.get());

        ThreadLocal<M> tl = new ThreadLocal<>();
        tl.set(new M());
        tl.remove();

    }
}
