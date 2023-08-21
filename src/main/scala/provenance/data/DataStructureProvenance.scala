package provenance.data

abstract class DataStructureProvenance(private var data: Any) extends LazyCloneableProvenance {
  override def hashCode(): Int = data.hashCode()
  
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: DataStructureProvenance =>
        data.equals(other.data)
      case _ => false
    }
  }
}
