package provenance.data

trait ProvenanceFactory {
  def create(ids: Long*): Provenance
}
