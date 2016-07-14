defmodule Schemata.Query.Insert do
  @moduledoc ""

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
    with:   :quorum
  ]

  @doc """
  Inserts the provided row into the table.

    insert into: "users", in: "my_keyspace",
      values: %{id: 1, name: "bob"}
      ttl: 8640000
      with: :quorum

    insert into: "my_keyspace.users",
      values: %{id: 1, name: "bob"},
      unique: true
  """
  @spec insert(Keyword.t) :: boolean
  def insert(query) do
    %__MODULE__{
      into: Keyword.fetch!(query, :into), in: query[:in],
      values: Keyword.fetch!(query, :values),
      unique: query[:unique],
      with: query[:with]
    }
    |> Query.run!
    |> Query.single_result
  end

  defimpl Schemata.Queryable do
    def to_query(insert) do
      %Query{
        statement:   statement(insert),
        values:      insert.values,
        keyspace:    insert.in,
        consistency: insert.with
      }
    end

    def statement(insert) do
      keys = Map.keys(insert.values)

      """
      INSERT INTO #{insert.into} (#{keys |> Enum.join(", ")}) \
      VALUES (#{keys |> length |> placeholders}) #{use_lwt(insert.unique)} \
      #{ttl_option(insert.ttl)}
      """
      |> String.trim
    end

    defp placeholders(n), do: n |> List.duplicate("?") |> Enum.join(", ")

    defp use_lwt(false), do: ""
    defp use_lwt(true), do: " IF NOT EXISTS"

    defp ttl_option(nil), do: ""
    defp ttl_option(_), do: " USING TTL ?"
  end
end
