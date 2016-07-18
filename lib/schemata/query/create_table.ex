defmodule Schemata.Query.CreateTable do
  @moduledoc ""

  import Schemata.Query.Helper
  alias Schemata.Query

  @type t :: %__MODULE__{
    named:       Query.table,
    columns:     Query.column_def,
    primary_key: Query.primary_key,
    order_by:    Query.ordering,
    in:          Query.keyspace,
    with:        Query.consistency_level
  }

  @enforce_keys [:named, :columns, :primary_key]
  defstruct [
    named:       nil,
    columns:     nil,
    primary_key: nil,
    order_by:    [],
    in:          nil,
    with:        nil
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_map(map) :: __MODULE__.t
  def from_map(map) do
    query_from_map map,
      take: [:named, :columns, :primary_key, :order_by, :in, :with],
      required: [:named, :columns, :primary_key],
      return: %__MODULE__{named: "bogus", columns: [], primary_key: []}
  end

  defimpl Schemata.Queryable do
    def statement(struct) do
      """
      CREATE TABLE IF NOT EXISTS #{struct.named} (\
      #{struct.columns |> column_strings} \
      PRIMARY KEY (#{struct.primary_key |> primary_key_string})\
      ) #{struct.order_by |> sorting_option_string}
      """
      |> String.trim
      |> squeeze
    end

    def values(_struct), do: %{}
    def keyspace(struct), do: struct.in
    def consistency(struct), do: struct.with
  end
end
