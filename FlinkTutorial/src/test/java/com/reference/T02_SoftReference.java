package com.reference;

import java.lang.ref.SoftReference;

public class T02_SoftReference {
    // execute this mini program with params -Xmx20M,  this means heap space  is 20M
    public static void main(String[] args) {
        SoftReference<byte[]> sr = new SoftReference<>(new byte[1024 * 1024 * 10]);
        // sr = null
         System.out.println(sr.get());
        System.gc();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(sr.get());
        System.gc();
        byte[] bytes = new byte[1024 * 1024 * 11];
        System.out.println(sr.get());
    }
}
