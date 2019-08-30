package com.telnet.spark

import org.joda.time
import com.sun.org.apache.xerces.internal.impl.dv.xs.AbstractDateTimeDV
import org.joda.time.{DateTime, DateTimeZone}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, ISODateTimeFormat}
import sun.java2d.pipe.SpanShapeRenderer.Simple

object Utilities {
  def transformDateTime(input: String): String = {
    val dtFormat: DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    val dt: DateTime = dtFormat.parseDateTime(input)
    dt.withZone(DateTimeZone.UTC).hourOfDay().roundFloorCopy().toString(ISODateTimeFormat.dateTimeNoMillis())
  }
}
