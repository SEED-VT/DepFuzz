package fuzzer

class Schema[T](val dataType: Schema.DataType, val range: (Int, Int) = null) {

}

object Schema extends Enumeration {
  type DataType = Value
  val TYPE_CATEGORICAL, TYPE_NUMERICAL, TYPE_OTHER = Value
}
