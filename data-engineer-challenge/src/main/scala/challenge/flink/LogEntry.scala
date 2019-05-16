package challenge.flink

import java.time.Instant

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.types.Row

import scala.util.Try

object LogEntry {
  val ELB_Logs: Array[(String, TypeInformation[_])] = Array(
    ("ts", Types.SQL_TIMESTAMP),
    ("elb_name", Types.STRING),
    ("request_ip", Types.STRING),
    ("request_port", Types.INT),
    ("backend_ip", Types.STRING),
    ("backend_port", Types.INT),
    ("request_processing_time", Types.DOUBLE),
    ("backend_processing_time", Types.DOUBLE),
    ("client_response_time", Types.DOUBLE),
    ("elb_response_code", Types.STRING),
    ("backend_response_code", Types.STRING),
    ("received_bytes", Types.LONG),
    ("sent_bytes", Types.LONG),
    ("request_verb", Types.STRING),
    ("url", Types.STRING),
    ("protocol", Types.STRING),
    ("user_agent", Types.STRING),
    ("ssl_cipher", Types.STRING),
    ("ssl_protocol", Types.STRING)
  )
  val fieldNameArray = ELB_Logs.map(x => x._1)
  val fieldTypeArray = ELB_Logs.map(x => x._2)
  val rowType = new RowTypeInfo(fieldTypeArray, fieldNameArray)

  def parse(textRow: String): Row = {
    // See: https://docs.aws.amazon.com/athena/latest/ug/elasticloadbalancer-classic-logs.html
    val regex =
      """([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) \"([^ ]*) ([^ ]*) (- |[^ ]*)\" (\"[^\"]*\") ([A-Z0-9-]+) ([A-Za-z0-9.-]*)$""".r
    val m = regex.findFirstMatchIn(textRow).get
    val row = new Row(19)
    row.setField(0, java.sql.Timestamp.from(Instant
      .parse(m.group(1).toString)))
    for (n <- 2 to m.groupCount) {
      if (fieldTypeArray(n - 1).equals(Types.INT)) {
        row.setField(n - 1, Try(m.group(n).toInt).getOrElse(null))
      }
      else if (fieldTypeArray(n - 1).equals(Types.DOUBLE)) {
        row.setField(n - 1, Try(m.group(n).toDouble).getOrElse(null))
      }
      else if (fieldTypeArray(n - 1).equals(Types.LONG)) {
        row.setField(n - 1, Try(m.group(n).toLong).getOrElse(null))
      }
      else {
        row.setField(n - 1, m.group(n))
      }
    }
    row
  }
}