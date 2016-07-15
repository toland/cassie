defmodule Schemata.Query.Helper do
  @moduledoc false

  @doc false
  def query_from_map(map, args) do
    for field <- args.required do
      unless map[field],
        do: raise ArgumentError, message: "Missing required field #{field}"
    end

    map
    |> Map.take(args.take)
    |> Enum.reject(fn {_, v} -> is_nil(v) end)
    |> Enum.into(args.return)
  end

  @doc false
  def squeeze(str) do
    Regex.replace(~r/  +/, str, " ")
  end

  @doc false
  def names(list), do: "(#{list |> Enum.join(", ")})"

  @doc false
  def columns(:all, default), do: default
  def columns([], default), do: default
  def columns(cols, _), do: Enum.join(cols, ", ")

  @doc false
  def conditions([]), do: ""
  def conditions([first | rest]) do
    List.foldl(rest, "WHERE #{first} = ?",
     fn (name, str) -> "#{str} AND #{name} = ?" end)
  end

  @doc false
  def limit(n) when is_integer(n) and n > 0, do: "LIMIT #{n}"
  def limit(_), do: ""

  @doc false
  def placeholders(1), do: "?"
  def placeholders(n), do: "?" |> List.duplicate(n) |> Enum.join(", ")

  @doc false
  def use_lwt(false), do: ""
  def use_lwt(true), do: "IF NOT EXISTS"

  @doc false
  def ttl_option(nil), do: ""
  def ttl_option(_), do: "USING TTL ?"

  @doc false
  def update_columns([first | rest]) do
    List.foldl(rest, "#{first} = ?",
     fn (name, str) -> "#{str}, #{name} = ?" end)
  end

  @doc false
  def object_name(name) do
    name
    |> to_string
    |> String.replace("_", " ")
    |> String.upcase
  end

  @doc ""
  def replication_strategy(:simple, factor) do
    "{'class': 'SimpleStrategy', 'replication_factor': #{factor}}"
  end
  def replication_strategy(:network_topology, dcs) do
    "{'class': 'NetworkTopologyStrategy'#{dc_factors(dcs, [])}}"
  end

  defp dc_factors([], factors), do: factors
  defp dc_factors([{dc, factor} | rest], factors) do
      factor_string = ", '#{dc}': #{factor}"
      dc_factors(rest, [factors | factor_string])
  end

  @doc false
  def column_strings(cols) do
    for {cname, ctype} <- cols, do: "#{cname} #{column_type(ctype)}, "
  end

  defp column_type({:map, type1, type2}), do: "map<#{type1},#{type2}>"
  defp column_type({coll, type}), do: "#{coll}<#{type}>"
  defp column_type(type), do: to_string(type)

  @doc false
  def primary_key_string(pk) when not is_list(pk), do: pk
  def primary_key_string(pklist) do
    pklist
    |> Enum.map(&primary_key_element/1)
    |> Enum.join(", ")
  end

  defp primary_key_element(columns) when is_list(columns), do: names(columns)
  defp primary_key_element(column), do: to_string(column)

  @doc false
  def sorting_option_string(nil), do: ""
  def sorting_option_string([]), do: ""
  def sorting_option_string(field) when is_atom(field),
    do: sorting_option_string([{field, :asc}])
  def sorting_option_string([{field, dir}]) do
    " WITH CLUSTERING ORDER BY (#{field} #{dir |> to_string |> String.upcase})"
  end

  @doc false
  def view_conditions(pk) when not is_list(pk), do: view_conditions([pk])
  def view_conditions([first | rest]) do
    List.foldl(rest, "WHERE #{first} IS NOT NULL",
     fn (name, str) -> "#{str} AND #{name} IS NOT NULL" end)
  end
end
