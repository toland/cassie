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
    with:     nil
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_opts(Keyword.t) :: __MODULE__.t
  def from_opts(opts) do
    query_from_opts opts,
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
    def keyspace(_struct), do: nil
    def consistency(struct), do: struct.with
  end
end
