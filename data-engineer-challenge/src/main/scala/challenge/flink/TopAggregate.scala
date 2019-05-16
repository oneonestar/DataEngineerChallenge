package challenge.flink

import challenge.flink.TopAggregateType.Aggregator
import org.apache.flink.api.common.functions.AggregateFunction

// No mutable TreeMap in Scala 2.11 (?!)
import scala.collection.immutable.TreeMap

object TopAggregateType {
  type Aggregator = TreeMap[Double, (String, Double)]
}

class TopAggregate(topN: Int) extends AggregateFunction[(String, Double),
  Aggregator,
  Seq[(String, Double)]] {
  override def createAccumulator() = new Aggregator()(Ordering[Double].reverse)

  override def add(value: (String, Double), accumulator: Aggregator): Aggregator = {
    if (accumulator.size < topN) {
      accumulator.+((value._2, value))
    }
    else if (accumulator.lastKey < value._2) {
      accumulator.+((value._2, value)).dropRight(1)
    }
    else {
      accumulator
    }
  }

  override def getResult(accumulator: Aggregator): Seq[(String, Double)] =
    accumulator.toList.map(x => x._2)

  override def merge(a: Aggregator, b: Aggregator): Aggregator = {
    val tmp = a.++(b)
    tmp.dropRight(topN - tmp.size)
  }
}

