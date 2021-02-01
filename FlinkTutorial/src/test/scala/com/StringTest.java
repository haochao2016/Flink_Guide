package com;

import org.junit.Test;
import static org.junit.Assert.*;

public class StringTest {

    @Test
    public void TestStringFormat () {
        String format = String.format("%d -- %d", 12, 123);
        System.out.println(format);
        assert(78 == 78);
        assertEquals("12 -- 123", format);
    }

}
