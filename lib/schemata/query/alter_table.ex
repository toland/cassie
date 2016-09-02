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
  @spec from_opts(Keyword.t) :: __MODULE__.t
  def from_opts(opts) do
    opts
    |> extract_op(Map.new(opts))
    |> query_from_opts([
         take: [:named, :column, :op, :in, :with],
         required: [:named, :column, :op],
         return: %__MODULE__{named: "bogus", column: :bogus, op: :drop}
       ])
  end

  defp extract_op(opts, %{alter: column, type: type}),
    do: opts
        |> Keyword.put(:column, column)
        |> Keyword.put(:op, {:alter, type})
  defp extract_op(opts, %{add: column, type: type}),
    do: opts
        |> Keyword.put(:column, column)
        |> Keyword.put(:op, {:add, type})
  defp extract_op(opts, %{rename: column, to: to}),
    do: opts
        |> Keyword.put(:column, column)
        |> Keyword.put(:op, {:rename, to})
  defp extract_op(opts, %{drop: column}),
    do: opts
        |> Keyword.put(:column, column)
        |> Keyword.put(:op, :drop)

  defimpl Schemata.Queryable do
    def statement(struct) do
      "ALTER TABLE #{struct.named} #{render_op(struct.column, struct.op)}"
    end

    defp render_op(column, {:alter, type}) do
      "ALTER #{column} TYPE #{render_type(type)}"
    end
    defp render_op(column, {:add, type}) do
      "ADD #{column} #{render_type(type)}"
    end
    defp render_op(column, {:rename, name}), do: "RENAME #{column} TO #{name}"
    defp render_op(column, :drop),           do: "DROP #{column}"

    defp render_type({collection, type}), do: "#{collection}<#{type}>"
    defp render_type(type), do: "#{type}"

    def values(_struct), do: %{}
    def keyspace(struct), do: struct.in
    def consistency(struct), do: struct.with
  end
end
