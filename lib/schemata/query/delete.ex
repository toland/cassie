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
    with:   :quorum
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_map(map) :: __MODULE__.t
  def from_map(map) do
    query_from_map map,
      take: [:from, :in, :values, :where, :with],
      required: [:from],
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
