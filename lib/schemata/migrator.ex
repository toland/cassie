defmodule Schemata.Migrator do
  @moduledoc ""

  require Logger

  use Timex
  use Schemata.CQErl
  alias Schemata.Migration
  import Schemata

  @keyspace "schemata"
  @table "migrations"

  @spec run(binary, :up | :down, list) :: {:ok, :applied | :already_applied}
                                        | {:error, term}
  def run(path, dir, opts \\ []) do
    ensure_migrations_table!

    migrations = migrations(path)
    case applicable_migrations(migrations, dir, opts) do
      [] ->
        Logger.info("Already #{dir}")
        purge(migrations)
        {:ok, :already_applied}

      to_apply  ->
        {time, result} = :timer.tc(Enum, :each, [to_apply, &migrate(&1, dir)])
        Logger.info("== Migrated in #{inspect(div(time, 10000)/10)}s")
        purge(migrations)
        result
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

  defp applicable_migrations(migrations, :up, _opts) do
    migrations
    |> Enum.filter(fn %Migration{applied_at: a} -> is_nil(a) end)
  end
  defp applicable_migrations(migrations, :down, opts) do
    n = Keyword.get(opts, :n, 1)

    migrations
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
    |> Map.merge(applied, &merge_from_file/3)
    |> Map.values
    |> Enum.sort(&sort_by_date/2)
  end

  defp transform(mig = %Migration{authored_at: a, description: d}) do
    {{d, a}, mig}
  end

  defp merge_from_file(_k, from_file, %Migration{applied_at: a}) do
    %Migration{from_file | applied_at: a}
  end

  defp sort_by_date(%Migration{authored_at: a}, %Migration{authored_at: b}) do
    a < b
  end

  def purge(migrations) do
    Code.unload_files(Code.loaded_files)
    for m <- migrations do
      :code.purge(m.module)
      :code.delete(m.module)
      :code.purge(m.module)
    end
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
    {:ok, :applied}
  rescue
    error ->
      :ok = Logger.error(Exception.message(error))
      :ok = Logger.info("There was an error while trying to migrate #{file}")
      {:error, Exception.message(error)}
  end

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
