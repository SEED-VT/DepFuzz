package taintedprimitives

import org.apache.spark.{HashPartitioner, Partitioner}

/**
  * Created by malig on 11/19/19.
  */
class CustomSymPartitioner(partitions: Int) extends Partitioner {
    require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

    def numPartitions: Int = partitions

    def getPartition(key: Any): Int = key match {
      case null => 0
      case _ => {
        val rawMod = key.hashCode % numPartitions
        rawMod + (if (rawMod < 0) numPartitions else 0)
      }
    }

    override def equals(other: Any): Boolean = other match {
      case h: HashPartitioner =>
        h.numPartitions == numPartitions
      case _ =>
        false
    }
    override def hashCode: Int = numPartitions
}
