package com.eumn;

import com.example_flink.apitest.sinktest.EnumTest;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.junit.Test;
import static org.junit.Assert.*;
//import org.junit.Assert;

public class TestEnum1 {

    @Test
    public void getEumn () {

        EnumTest df = EnumTest.DF;

//        assert (TimeCharacteristic.EventTime == TimeCharacteristic.ProcessingTime);
        assertEquals (TimeCharacteristic.EventTime,  TimeCharacteristic.IngestionTime);

    }
}
