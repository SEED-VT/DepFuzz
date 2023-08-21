package transformers

object Constants extends Enumeration {

  val MONITOR_CLASS = "_root_.monitoring.Monitors"

  val CONSOLIDATOR = s"$MONITOR_CLASS.finalizeProvenance(accTuples)"
  val KEY_FILTER = "filter"
  val KEY_JOIN = "join"
  val KEY_GBK = "groupByKey"
  val KEY_RBK = "reduceByKey"
  val KEY_PREDICATE = "predicate"

  val MAP_TRANSFORMS = Map(
    KEY_FILTER -> "monitorFilter",
    KEY_JOIN -> "monitorJoin",
    KEY_PREDICATE -> "monitorPredicate",
    KEY_GBK -> "monitorGroupByKey",
    KEY_RBK -> "monitorReduceByKey"
  ).mapValues(s => s"$MONITOR_CLASS.$s")

  val MAP_PRIM2SYM = Map(
    "Int" -> "TaintedInt",
    "String" -> "TaintedString",
    "Float" -> "TaintedFloat"
  )
}
