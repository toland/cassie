defmodule Schemata.Query.AlterTable do
  @moduledoc ""

  import Schemata.Query.Helper
  alias Schemata.Query

  @typep alter_op :: {:alter,  Query.datatype}
                   | {:add,    Query.datatype}
                   | {:rename, Query.column}
                   | :drop

  @type t :: %__MODULE__{
    named:       Query.table,
    column:      Query.column,
    op:          alter_op,
    in:          Query.keyspace,
    with:        Query.consistency_level
  }

  @enforce_keys [:named, :column, :op]
  defstruct [
    named:       nil,
    column:      nil,
    op:          nil,
    in:          nil,
    with:        nil
  ]

  @behaviour Schemata.Query

  @doc ""
  @spec from_map(map) :: __MODULE__.t
  def from_map(map) do
    new_map =
      map
      |> Map.take([:named, :in, :with])
      |> extract_op(map)

    query_from_map new_map,
      take: [:named, :column, :op, :in, :with],
      required: [:named, :column, :op],
      return: %__MODULE__{named: "bogus", column: :bogus, op: :drop}
  end

  defp extract_op(m, %{alter: column, type: type}),
    do: %{m | column: column, op: {:alter, type}}
  defp extract_op(m, %{add: column, type: type}),
    do: %{m | column: column, op: {:add, type}}
  defp extract_op(m, %{rename: column, to: to}),
    do: %{m | column: column, op: {:rename, to}}
  defp extract_op(m, %{drop: column}),
    do: %{m | column: column, op: :drop}

  defimpl Schemata.Queryable do
    def statement(struct) do
      "ALTER TABLE #{struct.named} #{render_op(struct.column, struct.op)}"
    end

    defp render_op(column, {:alter, type}),  do: "ALTER #{column} TYPE #{type}"
    defp render_op(column, {:add, type}),    do: "ADD #{column} #{type}"
    defp render_op(column, {:rename, name}), do: "RENAME #{column} TO #{name}"
    defp render_op(column, :drop),           do: "DROP #{column}"

    def values(_struct), do: %{}
    def keyspace(struct), do: struct.in
    def consistency(struct), do: struct.with
  end
end
