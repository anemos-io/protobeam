package io.anemos.protobeam.test;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import java.text.ParseException;

public class TimestampGenerator {

    private Timestamp[] data;
    private int point = 0;

    public TimestampGenerator() {
        try {
            data = new Timestamp[]{
                    Timestamps.parse("2018-11-28T12:34:56.123456789Z"),
                    Timestamps.parse("2018-01-01T12:00:00Z"),
                    Timestamps.parse("2018-01-01T12:00:00.001Z"),
                    Timestamps.parse("2018-01-01T12:00:00.000001Z")
            };
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public Timestamp nextTimestamp() {
        Timestamp val = data[point++];
        if (point >= data.length) point = 0;
        return val;
    }

}
