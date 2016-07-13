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

  alias Schemata.Migration

  require Record
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :cql_query, extract(:cql_query, from_lib: "cqerl/include/cqerl.hrl")

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
    create_keyspace = ~S"""
    CREATE KEYSPACE schemata_migrator
    WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 1};
    """

    create_table = ~S"""
    CREATE TABLE schemata_migrator.migrations (
      authored_at timestamp,
      description varchar,
      applied_at timestamp,
      PRIMARY KEY (authored_at, description)
    );
    """
    execute_idempotent(create_keyspace)
    execute_idempotent(create_table)
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
    |> Enum.map(fn file ->
      path |> Path.join(file) |> Migration.load
    end)
  end

  def migrations_applied do
    "SELECT * FROM schemata_migrator.migrations;"
    |> execute
    |> Enum.map(fn mig ->
      defaults = Map.delete(%Migration{}, :__struct__)
      mig
      |> Enum.into(defaults)
      |> Map.put(:__struct__, Migration)
    end)
  end

  defp migrate(mig = %Migration{filename: file}, :up) do
    :ok = Logger.info("== Running #{file}")
    mig.module.up
    query = """
    INSERT INTO schemata_migrator.migrations
      (authored_at, description, applied_at)
    VALUES
      (?, ?, ?);
    """
    values = %{
      authored_at: mig.authored_at,
      description: mig.description,
      applied_at: System.system_time(:milliseconds)
    }
    execute(query, values)
  rescue
    e in [Schemata.Migrator.CassandraError] ->
      :ok = Logger.error(Exception.message(e))
      :ok = Logger.info("There was an error while trying to migrate #{file}")
      migrate(mig, :down)
  end
  defp migrate(mig = %Migration{filename: file}, :down) do
    :ok = Logger.info("== Running #{file} backwards")
    mig.module.down
    query = """
    DELETE FROM schemata_migrator.migrations
    WHERE authored_at = ? AND description = ?;
    """
    values = %{authored_at: mig.authored_at, description: mig.description}
    execute(query, values)
  rescue
    e in [Schemata.Migrator.CassandraError] ->
      :ok = Logger.error(Exception.message(e))
      :ok = Logger.info("There was an error while trying to roll back #{file}")
  end

  defp execute(statement, values \\ %{}) do
    query = cql_query(statement: statement, values: values)
    case :cqerl.run_query(query) do
      {:ok, :void} -> :ok
      {:ok, result} -> :cqerl.all_rows(result)
      {:error, {8704, _, _}} -> []
      {:error, {code, msg, _}} -> raise CassandraError, [
       query: statement, error_message: msg, error_code: code
      ]
    end
  end

  defp execute_idempotent(query, opts \\ []) do
    if Keyword.get(opts, :log, false) do
      query_info =
        query
        |> String.split
        |> Enum.take(3)
        |> Enum.join(" ")
      Logger.info(query_info)
    end
    case :cqerl.run_query(query) do
      {:ok, _} -> :ok
      # Cannot add existing keyspace/table
      {:error, {9216, _, _}} -> :ok
      # Cannot drop keyspace
      {:error, {8960, _, _}} -> :ok
      # Cannot drop table
      # We match on the message because the code is also used for
      # Error: No keyspace specified
      {:error, {8704, "unconfigured table" <> _, _}} -> :ok
      {:error, {code, msg, _}} -> raise CassandraError, [
       error_message: msg, error_code: code
      ]
    end
  end
end
