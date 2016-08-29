defmodule Schemata.Query.CreateView do
  @moduledoc ""

  import Schemata.Query.Helper
  alias Schemata.Query

  @type t :: %__MODULE__{
    named:       Query.table,
    from:        Query.table,
    columns:     Query.columns,
    primary_key: Query.primary_key,
    order_by:    Query.ordering,
    in:          Query.keyspace,
    with:        Query.consistency_level,
    where:       Query.conditions
  }

  @enforce_keys [:named, :from, :primary_key]
  defstruct [
    named:       nil,
    from:        nil,
    columns:     :all,
    primary_key: nil,
    order_by:    [],
    in:          nil,
    with:        nil,
    where:       %{}
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_opts(Keyword.t) :: __MODULE__.t
  def from_opts(opts) do
    query_from_opts opts,
      take: [:named, :from, :columns, :primary_key, :order_by, :in, :with,
             :where],
      required: [:named, :from, :primary_key],
      return: %__MODULE__{named: "bogus", from: "bogus", primary_key: []}
  end

  defimpl Schemata.Queryable do
    def statement(struct) do
      """
      CREATE MATERIALIZED VIEW IF NOT EXISTS #{struct.named} AS \
      SELECT #{struct.columns |> columns("*")} FROM #{struct.from} \
      #{struct.primary_key |> view_pk_conditions} \
      #{struct.where |> view_conditions} \
      PRIMARY KEY (#{struct.primary_key |> primary_key_string}) \
      #{struct.order_by |> sorting_option_string}
      """
      |> String.trim
      |> squeeze
    end

    def values(_struct), do: %{}
    def keyspace(struct), do: struct.in
    def consistency(struct), do: struct.with
  end
end
