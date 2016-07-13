defmodule Schemata.Query.Select do
  @moduledoc ""

  alias Schemata.Query

  @enforce_keys [:from]
  defstruct [
    values: :all,
    from:   nil,
    in:     :none,
    where:  %{},
    limit:  :none,
    with:   :quorum
  ]

  @type t :: %__MODULE__{
    values: Query.columns,
    from:   Query.table,
    in:     Query.keyspace,
    where:  Query.conditions,
    limit:  Query.limit,
    with:   Query.consistency_level
  }

  defimpl Schemata.Queryable do
    def to_query(select) do
      %Query{
        statement:   statement(select),
        values:      select.where,
        keyspace:    select.in,
        consistency: select.with
      }
    end

    defp statement(select) do
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
