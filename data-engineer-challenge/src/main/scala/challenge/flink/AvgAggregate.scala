package challenge.flink

import org.apache.flink.api.common.accumulators.AverageAccumulator
import org.apache.flink.api.common.functions.AggregateFunction

/**
  * Calculate the average value of ._2 in a window
  */
class AvgAggregate extends AggregateFunction[(String, Double), AverageAccumulator, Double] {
  override def createAccumulator() = new AverageAccumulator

  override def add(value: (String, Double), accumulator: AverageAccumulator): AverageAccumulator = {
    accumulator.add(value._2)
    accumulator
  }

  override def getResult(accumulator: AverageAccumulator): Double =
    accumulator.getLocalValue

  override def merge(a: AverageAccumulator, b: AverageAccumulator): AverageAccumulator = {
    a.merge(b)
    a
  }
}

