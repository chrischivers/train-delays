package traindelays.scripts

import java.io.{FileInputStream, FileOutputStream, PrintWriter}

import scala.io.Source

object ScheduleFilteringScript extends App {

  val fileInputStream = new FileInputStream(getClass.getResource("/southern-toc-schedule-test").getPath)
  val outputStream    = new FileOutputStream("/tmp/test-schedule.json")
  val printWriter     = new PrintWriter(outputStream)
  try {
    for (line <- Source.fromInputStream(fileInputStream).getLines()) {
      if (line.contains("REDHILL") || line.contains("REIGATE"))
        if ((line.contains("G76481") && line.contains("JsonScheduleV1")) || line.contains("TiplocV1"))
          printWriter.write(line)
    }
  } finally {
    printWriter.close()
    outputStream.close()
    fileInputStream.close()
  }
}
