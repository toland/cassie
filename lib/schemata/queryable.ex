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

defimpl Schemata.Queryable, for: BitString do
  def statement(string), do: string
  def values(_string), do: %{}
  def keyspace(_string), do: nil
  def consistency(_string), do: nil
end
