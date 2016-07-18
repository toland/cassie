defmodule Schemata.Query.Truncate do
  @moduledoc ""

  import Schemata.Query.Helper
  alias Schemata.Query

  @type t :: %__MODULE__{
    table:  Query.table,
    in:     Query.keyspace,
    with:   Query.consistency_level
  }

  @enforce_keys [:table]
  defstruct [
    table:  nil,
    in:     nil,
    with:   nil
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_opts(Keyword.t) :: __MODULE__.t
  def from_opts(opts) do
    query_from_opts opts,
      take: [:table, :in, :with],
      required: [:table],
      return: %__MODULE__{table: "bogus"}
  end

  defimpl Schemata.Queryable do
    def statement(struct) do
      "TRUNCATE TABLE #{struct.table}"
    end

    def values(_struct), do: %{}
    def keyspace(struct), do: struct.in
    def consistency(struct), do: struct.with
  end
end
