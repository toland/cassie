defmodule Schemata.Migrator do
  @moduledoc ""

  require Logger

  defmodule MigrationError do
    @moduledoc ""

    defexception [message: nil]
  end

  defmodule CassandraError do
    @moduledoc ""

    defexception [
        error_message: nil,
        error_code: nil
      ]

    def message(%__MODULE__{error_message: message, error_code: code}) do
      "Error Code #{code}: #{message}"
    end
  end

  use Schemata.CQErl
  alias Schemata.Migration
  import Schemata.Query.Select, only: [select: 2]

  @keyspace "schemata"
  @table "migrations"

  def run(path, :up, _opts) do
    ensure_migrations_table!

    to_apply =
      path
      |> migrations
      |> Enum.filter(fn %Migration{applied_at: a} -> is_nil(a) end)

    case to_apply do
      [] -> Logger.info("Already up")
      to_apply  ->
        {time, _} = :timer.tc(Enum, :each, [to_apply, &migrate(&1, :up)])
        Logger.info("== Migrated in #{inspect(div(time, 10000)/10)}s")
    end
  end
  def run(path, :down, opts) do
    ensure_migrations_table!

    n = Keyword.get(opts, :n, 1)

    to_apply =
      path
      |> migrations
      |> Enum.filter(fn %Migration{applied_at: a} -> a end)
      |> Enum.reverse
      |> Enum.take(n)

    case to_apply do
      [] -> Logger.info("Already down")
      to_apply  ->
        {time, _} = :timer.tc(Enum, :each, [to_apply, &migrate(&1, :down)])
        Logger.info("== Migrated in #{inspect(div(time, 10000)/10)}s")
    end
  end

  def ensure_migrations_table! do
    create_keyspace = """
    CREATE KEYSPACE IF NOT EXISTS #{@keyspace}
    WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};
    """

    create_table = """
    CREATE TABLE IF NOT EXISTS #{@keyspace}.#{@table} (
      authored_at timestamp,
      description varchar,
      applied_at timestamp,
      PRIMARY KEY (authored_at, description)
    );
    """
    execute(create_keyspace)
    execute(create_table)
  end

  def migrations(path) do
    all =
      path
      |> migrations_available
      |> Enum.map(fn mig = %Migration{authored_at: a, description: d} ->
        {{d, a}, mig}
      end)
      |> Enum.into(%{})

    applied =
      migrations_applied
      |> Enum.map(fn mig = %Migration{authored_at: a, description: d} ->
        {{d, a}, mig}
      end)
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

  def migrations_available(path) do
    path
    |> File.ls!
    |> Enum.map(fn file -> path |> Path.join(file) |> Migration.load_file end)
  end

  def migrations_applied do
    select(:all, from: @table, in: @keyspace)
    |> Enum.map(&Migration.from_map/1)
  end

  defp migrate(migration = %Migration{filename: file}, direction) do
    :ok = Logger.info("== Migrating #{file} #{direction}")
    apply(migration.module, direction, [])
    {query, values} = make_db_query(migration, direction)
    execute(query, values)
  rescue
    e in [Schemata.Migrator.CassandraError] ->
      :ok = Logger.error(Exception.message(e))
      :ok = Logger.info("There was an error while trying to migrate #{file}")
      maybe_roll_back(direction, migration)
  end

  defp maybe_roll_back(_migration, :down), do: :ok
  defp maybe_roll_back(migration, :up), do: migrate(migration, :down)

  defp make_db_query(%Migration{authored_at: a, description: d}, :up) do
    query = """
    INSERT INTO #{@keyspace}.#{@table}
      (authored_at, description, applied_at)
    VALUES
      (?, ?, ?);
    """
    values = %{
      authored_at: a,
      description: d,
      applied_at: System.system_time(:milliseconds)
    }
    {query, values}
  end

  defp make_db_query(%Migration{authored_at: a, description: d}, :down) do
    query = """
    DELETE FROM #{@keyspace}.#{@table}
    WHERE authored_at = ? AND description = ?;
    """
    values = %{authored_at: a, description: d}
    {query, values}
  end

  defp execute(statement, values \\ %{}) do
    query = cql_query(statement: statement, values: values)
    case CQErl.run_query(query) do
      {:ok, :void} -> :ok
      {:ok, result} -> CQErl.all_rows(result)
      {:error, {8704, _, _}} -> []
      {:error, {code, msg, _}} -> raise CassandraError, [
       query: statement, error_message: msg, error_code: code
      ]
    end
  end
end
