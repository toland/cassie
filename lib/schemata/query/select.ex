defmodule Schemata.Query.Select do
  @moduledoc ""

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
    with:   :quorum
  ]

  @doc """
  Retrieves data from a table based on the parameters and returns all rows
  of the result set.

    select :all,
      from: "my_db.users",
      where: %{user_id: "bob"},
      limit: 1

    select :all,
      from: "users", in: "my_db",
      where: %{user_id: "bob"},
      limit: 1
      with: :quorum
  """
  @spec select(Query.columns, Keyword.t) :: Query.rows
  def select(columns, query) do
    %__MODULE__{
      values: columns,
      from: Keyword.fetch!(query, :from), in: query[:in],
      where: Keyword.get(query, :where, %{}),
      limit: query[:limit],
      with: query[:with]
    }
    |> Query.run!
    |> Query.all_rows
  end

  defimpl Schemata.Queryable do
    def to_query(select) do
      %Query{
        statement:   statement(select),
        values:      select.where,
        keyspace:    select.in,
        consistency: select.with
      }
    end

    def statement(select) do
      """
      SELECT #{columns(select.values, "*")} FROM #{select.from} \
      #{conditions(Map.keys(select.where))} #{limit(select.limit)}
      """
      |> String.trim
    end

    defp columns(:all, default), do: default
    defp columns([], default), do: default
    defp columns(cols, _), do: Enum.join(cols, ", ")

    defp conditions([]), do: ""
    defp conditions([first | rest]) do
      List.foldl(rest, "WHERE #{first} = ?",
       fn (name, str) -> "#{str} AND #{name} = ?" end)
    end

    defp limit(n) when is_integer(n) and n > 0, do: "LIMIT #{n}"
    defp limit(_), do: ""
  end
end
