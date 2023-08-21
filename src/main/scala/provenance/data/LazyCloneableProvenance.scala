package provenance.data

trait LazyCloneableProvenance extends Provenance {
  // internal flag: if this instance is cloned, we set it to false and any later calls to merge()
  // will first generate a 'safe' clone.
  private var safeToMerge = true
  override final def cloneProvenance(): Provenance = {
    safeToMerge = false
    this
  }
  
  // the non-lazy implementation
  protected def _cloneProvenance(): LazyCloneableProvenance
  
  /** Merges two provenance instances (in place), returning the current instance after merging. */
  override final def merge(other: Provenance): Provenance = {
    // no need to do anything!
    if(this.containsAll(other)) return this
    // return the other instance, but clone it since we may update it later.
    if(other.containsAll(this)) return other.cloneProvenance()
    
    val base = if(safeToMerge) this else _cloneProvenance()
    base._merge(other)
  }
  // merge assuming the input is non-lazy
  protected def _merge(other: Provenance): LazyCloneableProvenance
  
}
