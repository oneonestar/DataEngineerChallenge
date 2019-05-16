package challenge.flink

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.types.Row

import scala.collection.mutable

case class SessionInfo(
                        request_ip: String,
                        unique_url: mutable.Set[String],
                        earliest_time: Option[java.sql.Timestamp],
                        latest_time: Option[java.sql.Timestamp],
                        count: Long) {
  def this() = this(
    "",
    mutable.Set[String](),
    Option.empty,
    Option.empty,
    0
  )
}

/**
  * Aggregate the logs in a SessionWindow and form a [[SessionInfo]] per window.
  */
class UserSessionAggregate extends AggregateFunction[Row, SessionInfo, SessionInfo] {
  override def createAccumulator() = new SessionInfo

  override def add(value: Row, accumulator: SessionInfo): SessionInfo = {
    val valueTime = value.getField(0).asInstanceOf[java.sql.Timestamp]
    SessionInfo(
      value.getField(2).asInstanceOf[String],
      accumulator.unique_url.+=(value.getField(14).asInstanceOf[String]),
      accumulator.earliest_time.map(t => {
        if (t.before(valueTime)) t else valueTime
      }) orElse Option(valueTime),
      accumulator.latest_time.map(t => {
        if (t.after(valueTime)) t else valueTime
      }) orElse Option(valueTime),
      accumulator.count + 1
    )
  }

  override def getResult(accumulator: SessionInfo): SessionInfo =
    accumulator

  //TODO: Use Applicative functors => How...?
  def minTime(a: Option[java.sql.Timestamp], b: Option[java.sql.Timestamp]) = (a, b) match {
    case (Some(a), Some(b)) => Some(
      if (a.before(b)) a else b
    )
    case (None, Some(b)) => Some(b)
    case (Some(a), None) => Some(a)
    case (None, None) => None
  }

  def maxTime(a: Option[java.sql.Timestamp], b: Option[java.sql.Timestamp]) = (a, b) match {
    case (Some(a), Some(b)) => Some(
      if (a.after(b)) a else b
    )
    case (None, Some(b)) => Some(b)
    case (Some(a), None) => Some(a)
    case (None, None) => None
  }

  override def merge(a: SessionInfo, b: SessionInfo): SessionInfo = {
    SessionInfo(
      a.request_ip,
      a.unique_url.++(b.unique_url),
      minTime(a.earliest_time, b.earliest_time),
      maxTime(a.latest_time, b.latest_time),
      a.count + b.count
    )
  }
}

