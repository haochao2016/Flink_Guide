package test;

import org.openjdk.jol.info.ClassLayout;

public class ObjectTest {
    public static volatile int p1 = 9 ;
    // https://blog.csdn.net/SCDN_CP/article/details/86491792
    public static void main(String[] args) {
        /**
         *  View the object info,
         *  MarkWord is first 8 bytes, contains lock info and GC info  --- 64 bits
         *  class metadata is 8-11 bytes,                              --- 32 bits
         *
         *  Get the different info from below test case,  print the object info with or without lock.
         *  You can get the diff object header information.
         */


        Object o = new Object();
        System.out.println(ClassLayout.parseInstance(o).toPrintable());
        synchronized (o) {
            System.out.println(ClassLayout.parseInstance(o).toPrintable());
        }

        //0     4        (object header)                           d8 f2 e6 02 (11011000 11110010 11100110 00000010) (48689880)

    }
}
