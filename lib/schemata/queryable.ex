defprotocol Schemata.Queryable do
  @doc "Extracts a CQL statement from the query struct"
  def statement(struct)

  @doc "Extracts values from the query struct"
  def values(struct)

  @doc "Extracts the keyspace name from the query struct"
  def keyspace(struct)

  @doc "Extracts the consistency level from the query struct"
  def consistency(struct)
end
