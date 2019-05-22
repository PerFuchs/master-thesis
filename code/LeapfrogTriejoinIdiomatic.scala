class LeapfrogTriejoinIdiomatic(trieIterators: Map[Set[String], TrieIterator],
                                variableOrdering: Seq[String]) {
  // One LeapfrogJoin per variable with references to all TrieIterators$\label{line:lftjInitStart}$
  // which relationships have an attribute of the same name
  private val leapfrogJoins: Array[LeapfrogJoin]

  // A mapping of each variable to all TrieIterators related to it
  private val variable2TrieIterators: Map[String, Seq[TrieIterator]]

  private val maxDepth = variableOrdering.length - 1
  private var action = DOWN_ACTION // The important line $\label{myline}$
  private var depth = -1
  private var bindings = Array.fill(variableOrdering.size)(-1L)
  var atEnd: Boolean = trieIterators.values.exists(i => i.atEnd)//$\label{line:lftjInitEnd}$

  def moveToNextTuple(): Array[Long] = {//$\label{line:moveToNextTuple}$
    if (action == NEXT_ACTION) action = nextAction()
    do {
      if (action == DOWN_ACTION) {
        triejoinOpen()
        if (leapfrogJoins(depth).atEnd) {
          action = UP_ACTION
        } else {
          bindings(depth) = leapfrogJoins(depth).key
          action = if (depth == maxDepth) NEXT_ACTION else DOWN_ACTION
        }
      } else if (action == UP_ACTION) {
        if (depth == 0) {
          atEnd = true
        } else {
          triejoinUp()
          action = if (leapfrogJoins(depth).atEnd) UP_ACTION else nextAction()
        }
      }
    } while (!((depth == maxDepth && bindings(maxDepth) != -1) || atEnd))
    bindings
  }
}