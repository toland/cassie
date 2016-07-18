defmodule Schemata.Query.CreateIndex do
  @moduledoc ""

  import Schemata.Query.Helper
  alias Schemata.Query

  @type t :: %__MODULE__{
    on:     Query.table,
    keys:   list,
    in:     Query.keyspace,
    with:   Query.consistency_level
  }

  @enforce_keys [:on, :keys]
  defstruct [
    on:     nil,
    keys:   [],
    in:     nil,
    with:   nil
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_opts(Keyword.t) :: __MODULE__.t
  def from_opts(opts) do
    query_from_opts opts,
      take: [:on, :keys, :in, :with],
      required: [:on, :keys],
      return: %__MODULE__{on: "bogus", keys: []}
  end

  defimpl Schemata.Queryable do
    def statement(struct) do
      """
      CREATE INDEX IF NOT EXISTS ON #{struct.on} #{struct.keys |> names}
      """
      |> String.trim
    end

    def values(_struct), do: %{}
    def keyspace(struct), do: struct.in
    def consistency(struct), do: struct.with
  end
end
