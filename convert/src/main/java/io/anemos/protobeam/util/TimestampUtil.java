package io.anemos.protobeam.util;

import com.google.protobuf.Timestamp;
import org.joda.time.Instant;

public class TimestampUtil {

  public static Timestamp from(Instant instant) {
    long totalMillis = instant.getMillis();
    long seconds = totalMillis / 1000;
    int ns = (int) (totalMillis % 1000 * 1000000);
    return Timestamp.newBuilder().setSeconds(seconds).setNanos(ns).build();
  }

  public static Timestamp fromBQ(String canonicalTimestamp) {
    if (canonicalTimestamp == null || canonicalTimestamp.length() == 0) {
      return null;
    }
    // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]
    int nano = 0;

    if (canonicalTimestamp.endsWith("UTC")) {
      canonicalTimestamp = canonicalTimestamp.substring(0, canonicalTimestamp.length() - 4);
    } else if (canonicalTimestamp.endsWith("Z")) {
      canonicalTimestamp = canonicalTimestamp.substring(0, canonicalTimestamp.length() - 1);
    }

    int dotIndex = canonicalTimestamp.indexOf('.');
    String dateTimePart;
    if (dotIndex > 0) {
      dateTimePart = canonicalTimestamp.substring(0, dotIndex);
      String nanoPart = canonicalTimestamp.substring(dotIndex + 1);
      nano = Integer.parseInt(nanoPart);
      if (nanoPart.length() == 3) {
        nano *= 10000000;
      } else if (nanoPart.length() == 6) {
        nano *= 1000;
      }
    } else {
      dateTimePart = canonicalTimestamp;
    }

    dateTimePart = dateTimePart.replace(' ', 'T') + "Z";
    long seconds = Instant.parse(dateTimePart).getMillis() / 1000;

    return Timestamp.newBuilder().setSeconds(seconds).setNanos(nano).build();
  }

  public static Instant from(Timestamp timestamp) {
    return new Instant(timestamp.getSeconds() * 1000 + timestamp.getNanos() / 1000000);
  }
}
