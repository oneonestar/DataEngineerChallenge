package challenge.flink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

object StreamingJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Max out of order delay of the log
    // TODO: Handle late data after watermark instead of holds back the watermark
    val maxDelay = Time.days(5)

    val myStream: DataStream[Row] = env
      .readTextFile("../data/2015_07_22_mktplace_shop_web_log_sample.log.gz")
      .map(l => LogEntry.parse(l))(LogEntry.rowType)
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[Row](maxDelay) {
          override def extractTimestamp(element: Row): Long = {
            element.getField(0).asInstanceOf[java.sql.Timestamp].getTime
          }
        })

    // Sessionize the web log by IP.
    val sessionDuration = Time.minutes(15)
    val aggregatedStream = myStream
      .keyBy(k => k.getField(2).asInstanceOf[String])
      .window(EventTimeSessionWindows.withGap(sessionDuration))
      .aggregate(new UserSessionAggregate)


    // 1. All page hits by visitor/IP during a session.
    aggregatedStream
      .map(r => (r.request_ip, r.count))
      .map(r => r.toString()).addSink(createFileSink("output/task1"))

    // Need a big global window to aggregate the results
    val globalAggregateDuration = Time.days(5)

    // 2. Determine the average session time
    def timediff(t1: java.sql.Timestamp, t2: java.sql.Timestamp) =
      math.abs(t1.getTime - t2.getTime) / 1000.0

    aggregatedStream
      .map(r => (r.request_ip, timediff(r.latest_time.get, r.earliest_time.get)))
      .windowAll(TumblingEventTimeWindows.of(globalAggregateDuration))
      .aggregate(new AvgAggregate)
      .map(r => r.toString()).addSink(createFileSink("output/task2"))


    // 3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
    aggregatedStream
      .map(r => (r.request_ip, r.unique_url.size))
      .map(r => r.toString()).addSink(createFileSink("output/task3"))


    // 4. Find the most engaged users, ie the IPs with the longest session times
    aggregatedStream
      .map(r => (r.request_ip, timediff(r.latest_time.get, r.earliest_time.get)))
      .keyBy(0)
//      .window(EventTimeSessionWindows.withGap(globalAggregateDuration))
//      .sum(1)
      .windowAll(TumblingEventTimeWindows.of(globalAggregateDuration))
      .aggregate(new TopAggregate(10))
      .flatMap(x => for (fn <- x) yield fn)
      .map(r => r.toString()).addSink(createFileSink("output/task4"))

    env.execute("PayPay Data Engineer Challenge")
  }

  private def createFileSink(path: String) = {
    StreamingFileSink
      .forRowFormat(new Path(path), new SimpleStringEncoder[String]("UTF-8"))
      .build()
  }
}
