defmodule Schemata.Query.Update do
  @moduledoc ""

  alias Schemata.Query

  @type t :: %__MODULE__{
    table:  Query.table,
    in:     Query.keyspace,
    set:    Query.values,
    where:  Query.values,
    with:   Query.consistency_level
  }

  @enforce_keys [:table, :set]
  defstruct [
    table:  nil,
    in:     nil,
    set:    nil,
    where:  %{},
    with:   :quorum
  ]

  @doc """
  Updates rows in a table with the provided values.

    update "users", in: "my_db",
      set: %{email: "bob@company.com"}
      where: %{user_name: "bob"}
  """
  @spec update(Query.table, Keyword.t) :: :ok
  def update(table, query) do
    %__MODULE__{
      table: table, in: query[:in],
      set: Keyword.fetch!(query, :set),
      where: query[:where],
      with: query[:with]
    }
    |> Query.run!
    :ok
  end

  defimpl Schemata.Queryable do
    def to_query(update) do
      values = Map.merge(update.set, update.where)
      %Query{
        statement:   statement(update),
        values:      values,
        keyspace:    update.in,
        consistency: update.with
      }
    end

    def statement(update) do
      """
      UPDATE #{update.table} SET #{update.set |> Map.keys |> update_columns} \
      #{conditions(update.where |> Map.keys)}
      """
      |> String.trim
    end

    defp update_columns([first | rest]) do
      List.foldl(rest, "#{first} = ?",
       fn (name, str) -> "#{str}, #{name} = ?" end)
    end

    defp conditions([]), do: ""
    defp conditions([first | rest]) do
      List.foldl(rest, "WHERE #{first} = ?",
       fn (name, str) -> "#{str} AND #{name} = ?" end)
    end
  end
end
