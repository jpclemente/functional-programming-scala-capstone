package observatory.util

import java.time.{Duration, Instant}


class TimeRecord() {
  var durations: List[Long] = Nil
  var times: List[Instant] = List(Instant.now())
  println("started recording time lapsing at " + times.head)

  def record(time: Instant): Unit = {
    this.durations = this.times match {
      case Nil => durations
      case h::_ => Duration.between(h, time).getSeconds::durations
    }
    this.times = time::this.times
  }

  def stepFinished(time: Instant, stepName: String): Unit = {
    this.record(time)
    println(stepName + " completed in: " + this.durations.head + " seconds")
  }

  def processFinished(time: Instant): Unit = {
    this.record(time)
    val totalElapsed = Duration.between(times.reverse.head, time).abs().getSeconds
    println("Total time elapsed: " + totalElapsed + " seconds")
  }
}
