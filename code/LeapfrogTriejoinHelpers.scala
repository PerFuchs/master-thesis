private def nextAction(): Int = {
  leapfrogJoins(depth).leapfrogNext()
  if (leapfrogJoins(depth).atEnd) {
    UP_ACTION
  } else {
    bindings(depth) = leapfrogJoins(depth).key
    if (depth == maxDepth) NEXT_ACTION else DOWN_ACTION
  }
}

private def triejoinOpen(): Unit = {
  depth += 1
  val trieIterators = variable2TrieIterators(variableOrdering(depth)).foreach(_.open())
  leapfrogJoins(depth).init()
}

private def triejoinUp(): Unit = {
  variable2TrieIterators(variableOrdering(depth)).foreach(_.up())
  bindings(depth) = -1
  depth -= 1
}