defmodule Schemata.Query.Builder do

  @spec select_query(atom, list(atom) | :all, list(atom), non_neg_integer) :: binary
  def select_query(table, columns \\ :all, keys \\ [], limit \\ 0) do
    """
    SELECT #{columns(columns, "*")} FROM #{table} \
    #{conditions(keys)} #{limit(limit)}
    """
    |> String.trim
  end

  defp columns(:all, default), do: default
  defp columns([], default), do: default
  defp columns(cols, _), do: Enum.join(cols, ", ")

  defp conditions([]), do: ""
  defp conditions([first|rest]) do
    List.foldl(rest, "WHERE #{first} = ?",
     fn (name, str) -> "#{str} AND #{name} = ?" end)
  end

  defp limit(n) when is_integer(n) and n > 0, do: "LIMIT #{n}"
  defp limit(_), do: ""

end
