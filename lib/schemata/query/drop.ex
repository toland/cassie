defmodule Schemata.Query.Drop do
  @moduledoc ""

  import Schemata.Query.Helper
  alias Schemata.Query

  @type t :: %__MODULE__{
    object: :keyspace | :table | :index | :materialized_view,
    named:  Query.table,
    in:     Query.keyspace,
    with:   Query.consistency_level
  }

  @enforce_keys [:object, :named]
  defstruct [
    object: nil,
    named:  nil,
    in:     nil,
    with:   :quorum
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_map(map) :: __MODULE__.t
  def from_map(map) do
    query_from_map map,
      take: [:object, :named, :in, :with],
      required: [:object, :named],
      return: %__MODULE__{object: :table, named: "bogus"}
  end

  defimpl Schemata.Queryable do
    def statement(struct) do
      """
      DROP #{struct.object |> object_name} IF EXISTS #{struct.named}
      """
      |> String.trim
    end

    def values(_struct), do: %{}
    def keyspace(struct), do: struct.in
    def consistency(struct), do: struct.with
  end
end
