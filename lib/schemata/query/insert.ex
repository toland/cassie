defmodule Schemata.Query.Insert do
  @moduledoc ""

  import Schemata.Query.Helper
  alias Schemata.Query

  @type t :: %__MODULE__{
    into:   Query.table,
    in:     Query.keyspace,
    values: Query.values,
    unique: boolean,
    ttl:    integer,
    with:   Query.consistency_level
  }

  @enforce_keys [:into, :values]
  defstruct [
    into:   nil,
    in:     nil,
    values: nil,
    unique: false,
    ttl:    nil,
    with:   nil
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_opts(Keyword.t) :: __MODULE__.t
  def from_opts(opts) do
    query_from_opts opts,
      take: [:into, :in, :values, :unique, :ttl, :with],
      required: [:into, :values],
      return: %__MODULE__{into: "bogus", values: %{}}
  end

  defimpl Schemata.Queryable do
    def statement(struct) do
      keys = Map.keys(struct.values)

      """
      INSERT INTO #{struct.into} (#{keys |> Enum.join(", ")}) \
      VALUES (#{keys |> length |> placeholders}) #{use_lwt(struct.unique)} \
      #{ttl_option(struct.ttl)}
      """
      |> String.trim
      |> squeeze
    end

    def values(struct), do: struct.values
    def keyspace(struct), do: struct.in
    def consistency(struct), do: struct.with
  end
end
