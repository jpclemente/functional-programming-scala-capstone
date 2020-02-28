package observatory.util

import java.time.Instant

class ProcessMonitor(processName: String, nSteps: Int) {
  var computed = 0
  val timer = new TimeRecord()

  def stepDone(now: Instant): Unit = {
    this.computed += 1
    val progression = 100 * this.computed / nSteps
    if (this.computed % nSteps == 0) {
      this.timer.stepFinished(now, progression + "%")
    }
    if (progression == 100) this.timer.processFinished(now)
  }
}
