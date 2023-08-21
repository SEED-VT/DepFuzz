package provenance.data
import scala.collection.mutable.ListBuffer

class DummyProvenance private extends Provenance {

  override def hashCode(): Int = 0

  override def equals(obj: Any): Boolean = {
    obj match {
      case _: DummyProvenance => true
      case _ => false
    }
  }
  override def cloneProvenance(): Provenance = this

  override def merge(other: Provenance): Provenance = other.cloneProvenance()

  override def count: Int = 0

  override def estimateSize: Long = 0L
  override def toString(): String = {
    s"${this.getClass.getSimpleName}: [n/a]"
  }
  
  override def containsAll(other: Provenance): Boolean = {
    other.isInstanceOf[DummyProvenance]
  }

  override def getProvenanceAsSeq(): List[Int] = {
    List(1,2,3)
  }

  override def convertToTuples: ListBuffer[(Int, Int, Int)] = ???
}


object DummyProvenance extends ProvenanceFactory {
  private val instance = new DummyProvenance
  override def create(ids: Long*): Provenance = instance
}