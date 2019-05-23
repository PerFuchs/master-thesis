class LeapfrogJoinIdiomatic(var iterators: Array[LinearIterator]) {
  var atEnd: Boolean = false
  var p = 0
  var key = 0L

  def init(): Unit = {
    atEnd = iterators.exists(li => li.atEnd)
    p = 0
    key = -1

    iterators.sortBy(_.key)
    leapfrogSearch()
  }


  def leapfrogNext(): Unit = {
    iterators(p).next()
    p = (p + 1) % iterators.length
    leapfrogSearch()
  }

  def leapfrogLeastUpperBound(key: Long): Unit = {
    iterators(p).leastUpperBound(key)
    p = (p + 1) % iterators.length
    leapfrogSearch()
  }

  private def leapfrogSearch(): Unit = {
    if (!iterators(p).atEnd) {
      var max = iterators((p - 1) % iterators.length).key
      var min = iterators(p).key

      while (min != max && !iterators(p).atEnd) {
        iterators(p).leastUpperBound(max)
        max = iterators(p).key
        p = (p + 1) % iterators.length
        min = iterators(p).key
      }
      key = min
    }
    atEnd = iterators(p).atEnd
  }
}
