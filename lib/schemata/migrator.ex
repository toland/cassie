defmodule Schemata.Migrator do
  @moduledoc ""

  require Logger

  defmodule MigrationError do
    @moduledoc ""

    defexception [message: nil]
  end

  use Timex
  use Schemata.CQErl
  alias Schemata.Migration
  import Schemata

  @keyspace "schemata"
  @table "migrations"

  def run(path, dir, opts \\ []) do
    ensure_migrations_table!

    case applicable_migrations(path, dir, opts) do
      [] -> Logger.info("Already #{dir}")
      to_apply  ->
        {time, _} = :timer.tc(Enum, :each, [to_apply, &migrate(&1, :up)])
        Logger.info("== Migrated in #{inspect(div(time, 10000)/10)}s")
    end
  end

  def ensure_migrations_table! do
    create_keyspace @keyspace
    create_table @table, in: @keyspace,
      columns: [
        authored_at: :timestamp,
        description: :text,
        applied_at:  :timestamp
      ],
      primary_key: [:authored_at, :description]
  end

  defp applicable_migrations(path, :up, _opts) do
    path
    |> migrations
    |> Enum.filter(fn %Migration{applied_at: a} -> is_nil(a) end)
  end
  defp applicable_migrations(path, :down, opts) do
    n = Keyword.get(opts, :n, 1)

    path
    |> migrations
    |> Enum.filter(fn %Migration{applied_at: a} -> a end)
    |> Enum.reverse
    |> Enum.take(n)
  end

  def migrations(path) do
    all =
      path
      |> migrations_available
      |> Enum.map(&transform/1)
      |> Enum.into(%{})

    applied =
      migrations_applied
      |> Enum.map(&transform/1)
      |> Enum.into(%{})

    all
    |> Map.merge(applied, fn _k, from_file, %Migration{applied_at: a}  ->
      %Migration{from_file | applied_at: a}
    end)
    |> Map.values
    |> Enum.sort(fn %Migration{authored_at: a}, %Migration{authored_at: b} ->
      a < b
    end)
  end

  defp transform(mig = %Migration{authored_at: a, description: d}) do
    {{d, a}, mig}
  end

  def migrations_available(path) do
    path
    |> File.ls!
    |> Enum.map(fn file -> path |> Path.join(file) |> Migration.load_file end)
  end

  def migrations_applied do
    :all
    |> select(from: @table, in: @keyspace)
    |> Enum.map(&Migration.from_map/1)
  end

  defp migrate(migration = %Migration{filename: file}, direction) do
    :ok = Logger.info("== Migrating #{file} #{direction}")
    apply(migration.module, direction, [])
    update_db(migration, direction)
  rescue
    e in [Schemata.Migrator.CassandraError] ->
      :ok = Logger.error(Exception.message(e))
      :ok = Logger.info("There was an error while trying to migrate #{file}")
      maybe_roll_back(migration, direction)
  end

  defp maybe_roll_back(_migration, :down), do: :ok
  defp maybe_roll_back(migration, :up), do: migrate(migration, :down)

  defp update_db(%Migration{authored_at: a, description: d}, :up) do
    insert into: @table, in: @keyspace,
      values: %{
        description: d,
        authored_at: a,
        applied_at: timestamp
      }
  end
  defp update_db(%Migration{authored_at: a, description: d}, :down) do
    delete from: @table, in: @keyspace,
      where: %{authored_at: a, description: d}
  end

  defp timestamp, do: System.system_time(:milliseconds)
end
