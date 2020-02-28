package observatory

object Common {
  def customAssert[T](expected: T, result: T): Unit = {
    assert(expected equals result, "expected = " + expected + ", result = " + result)
  }
}
