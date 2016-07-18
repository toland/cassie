defmodule Schemata.Query.Select do
  @moduledoc ""

  import Schemata.Query.Helper
  alias Schemata.Query

  @type t :: %__MODULE__{
    values: Query.columns,
    from:   Query.table,
    in:     Query.keyspace,
    where:  Query.conditions,
    limit:  Query.limit,
    with:   Query.consistency_level
  }

  @enforce_keys [:from]
  defstruct [
    values: :all,
    from:   nil,
    in:     nil,
    where:  %{},
    limit:  nil,
    with:   nil
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_opts(Keyword.t) :: __MODULE__.t
  def from_opts(opts) do
    query_from_opts opts,
      take: [:values, :from, :in, :where, :limit, :with],
      required: [:from],
      return: %__MODULE__{from: "bogus"}
  end

  defimpl Schemata.Queryable do
    def statement(struct) do
      """
      SELECT #{columns(struct.values, "*")} FROM #{struct.from} \
      #{struct.where |> Map.keys |> conditions} #{limit(struct.limit)}
      """
      |> String.trim
    end

    def values(struct), do: struct.where
    def keyspace(struct), do: struct.in
    def consistency(struct), do: struct.with
  end
end
