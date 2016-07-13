defprotocol Schemata.Queryable do
  @doc "Converts the struct to a CQL query"
  def to_query(struct)
end
