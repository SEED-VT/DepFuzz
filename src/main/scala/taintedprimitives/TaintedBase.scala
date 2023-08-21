package taintedprimitives

import provenance.data.Provenance


abstract class TaintedBase(private var prov: Provenance) extends Serializable{
  // The provenance object may be used in other TaintedBase or even row-level operations, so we
  // should clone it to avoid potential issues with sharing the same instance.
  // (This should hopefully leverage lazy cloning functionality under the covers to reduce
  // overheads)
  // This idea applies to any method that accepts or produces provenance for use outside the
  // TaintedBase API.
  prov = prov.cloneProvenance()
  
  def getProvenance() : Provenance = prov.cloneProvenance() // the provenance object may now be
  // shared across other instances.
  def setProvenance(p : Provenance) = prov = p.cloneProvenance()
  
  // TODO: Implement the influence/rank function here
  /** Mutate the current provenance instance to merge in the arguments. For a non-mutating method
    * that simply produces a new provenance object, see newProvenance().
    * This method is meant for usage in methods that do not produce new TaintedBase entities, but are
    * still relevant for provenance tracking (e.g. numerical '>').
    */
  protected def mergeProvenance(otherProv : Provenance*): Provenance = {
    prov = otherProv.foldLeft(prov)({case (prov, other) => prov.merge(other)})
    prov
  }
  
  /** Generate a new provenance instance, typically used when creating a new TaintedBase. For a
    * mutating method to replace the current provenance, see mergeProvenance(). */
  def newProvenance(otherProv: Provenance*): Provenance = {
    val result = otherProv.foldLeft(prov.cloneProvenance())({case (prov, other) => prov.merge(other)})
    result
  }

}
