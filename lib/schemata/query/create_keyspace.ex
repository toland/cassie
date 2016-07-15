defmodule Schemata.Query.CreateKeyspace do
  @moduledoc ""

  import Schemata.Query.Helper
  alias Schemata.Query

  @type t :: %__MODULE__{
    named:    Query.keyspace,
    strategy: Query.ks_strategy,
    factor:   Query.ks_factor,
    with:     Query.consistency_level
  }

  @enforce_keys [:named]
  defstruct [
    named:    nil,
    strategy: :simple,
    factor:   1,
    with:     :quorum
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_map(map) :: __MODULE__.t
  def from_map(map) do
    query_from_map map,
      take: [:named, :strategy, :factor, :with],
      required: [:named],
      return: %__MODULE__{named: "bogus"}
  end

  defimpl Schemata.Queryable do
    def statement(struct) do
      """
      CREATE KEYSPACE IF NOT EXISTS #{struct.named} \
      WITH REPLICATION = #{replication_strategy(struct.strategy, struct.factor)}
      """
      |> String.trim
    end

    def values(_struct), do: %{}
    def keyspace(struct), do: struct.in
    def consistency(struct), do: struct.with
  end
end
