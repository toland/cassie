defmodule Schemata.Query.Delete do
  @moduledoc ""

  import Schemata.Query.Helper
  alias Schemata.Query

  @type t :: %__MODULE__{
    from:   Query.table,
    in:     Query.keyspace,
    values: Query.columns,
    where:  Query.values,
    with:   Query.consistency_level
  }

  @enforce_keys [:from]
  defstruct [
    from:   nil,
    in:     nil,
    values: :all,
    where:  %{},
    with:   nil
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_opts(Keyword.t) :: __MODULE__.t
  def from_opts(opts) do
    query_from_opts opts,
      take: [:from, :in, :values, :where, :with],
      required: [:from, :where],
      return: %__MODULE__{from: "bogus"}
  end

  defimpl Schemata.Queryable do
    def statement(struct) do
      """
      DELETE #{columns(struct.values, "")} FROM #{struct.from} \
      #{conditions(struct.where |> Map.keys)}
      """
      |> String.trim
      |> squeeze
    end

    def values(struct), do: struct.where
    def keyspace(struct), do: struct.in
    def consistency(struct), do: struct.with
  end
end
