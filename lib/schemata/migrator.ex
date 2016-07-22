defmodule Schemata.Migrator do
  @moduledoc ""

  use Timex
  use GenServer
  use Schemata.CQErl
  import Schemata
  alias Schemata.Migration
  require Logger

  defmodule State do
    defstruct [
      path:       nil,
      keyspace:   nil,
      table:      nil,
      migrations: []
    ]
  end

  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, term}
  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: Migrator)
  end

  @spec stop() :: :ok
  def stop do
    GenServer.stop(Migrator, :normal)
  end

  @spec migrations_path :: binary
  def migrations_path do
    GenServer.call(Migrator, :migrations_path)
  end

  @spec load_migrations(binary) :: :ok | {:error, term}
  def load_migrations(path \\ nil) do
    GenServer.call(Migrator, {:load_migrations, path})
  end

  @spec list_migrations(:all | :available | :applied) :: [Migration.t]
  def list_migrations(limit \\ :all) do
    GenServer.call(Migrator, {:list_migrations, limit})
  end

  @spec migrate(:up | :down, pos_integer) ::
    {:ok, :applied | :already_applied} | {:error, term}
  def migrate(dir, n \\ 1) do
    GenServer.call(Migrator, {:migrate, dir, n})
  end


  # -------------------------------------------------------------------------
  # GenServer callbacks

  def init(args) do
    defaults = %{
      migrations: [],
      keyspace: Application.fetch_env!(:schemata, :migrations_keyspace),
      table: Application.fetch_env!(:schemata, :migrations_table),
      path: Application.fetch_env!(:schemata, :migrations_path)
    }

    state =
      args
      |> Keyword.take([:path, :keyspace, :table])
      |> Enum.into(defaults)
      |> Map.put(:__struct__, %State{}.__struct__)

    ensure_migrations_table!(state.keyspace, state.table)

    {:ok, state}
  end

  def handle_call(:migrations_path, _from, state) do
    {:reply, state.path, state}
  end

  def handle_call({:load_migrations, nil}, from, state) do
    handle_call({:load_migrations, state.path}, from, state)
  end

  def handle_call({:load_migrations, path}, _from, state) do
    all = load_migrations_from_files(path)
    applied = load_migrations_from_db(state.keyspace, state.table)
    migrations = merge_migrations(all, applied)
    {:reply, :ok, %State{state | path: path, migrations: migrations}}
  end

  def handle_call({:list_migrations, :all}, _from, state) do
    {:reply, state.migrations, state}
  end

  def handle_call({:list_migrations, :applied}, _from, state) do
    {:reply, applied_migrations(state.migrations), state}
  end

  def handle_call({:list_migrations, :available}, _from, state) do
    {:reply, available_migrations(state.migrations), state}
  end

  def handle_call({:migrate, dir, n}, _from, state) do
    migrations = applicable_migrations(state.migrations, dir, n)
    result = run_migrations(migrations, dir, state)
    applied = load_migrations_from_db(state.keyspace, state.table)
    migrations = merge_migrations(state.migrations, applied)
    {:reply, result, %State{state | migrations: migrations}}
  end

  def terminate(_reason, %State{migrations: migrations}) do
    Code.unload_files(Code.loaded_files)
    for m <- migrations do
      :code.purge(m.module)
      :code.delete(m.module)
      :code.purge(m.module)
    end
  end


  # -------------------------------------------------------------------------
  # Private helper functions

  defp ensure_migrations_table!(keyspace, table) do
    create_keyspace keyspace
    create_table table, in: keyspace,
      columns: [
        authored_at: :timestamp,
        description: :text,
        applied_at:  :timestamp
      ],
      primary_key: [:authored_at, :description]
  end

  defp load_migrations_from_files(path) do
    path
    |> File.ls!
    |> Enum.map(fn file -> path |> Path.join(file) |> Migration.load_file end)
  end

  defp load_migrations_from_db(keyspace, table) do
    :all
    |> select(from: table, in: keyspace)
    |> Enum.map(&Migration.from_map/1)
  end

  defp merge_migrations(all, applied) do
    all =
      all
      |> Enum.map(fn m -> %Migration{m | applied_at: nil} end)
      |> Enum.map(&transform/1)
      |> Enum.into(%{})

    applied =
      applied
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

  defp available_migrations(migrations) do
    migrations
    |> Enum.filter(fn %Migration{applied_at: a} -> is_nil(a) end)
  end

  defp applied_migrations(migrations) do
    migrations
    |> Enum.filter(fn %Migration{applied_at: a} -> a end)
  end

  defp applicable_migrations(migrations, :up, _n) do
    available_migrations(migrations)
  end
  defp applicable_migrations(migrations, :down, n) do
    applied_migrations(migrations)
    |> Enum.reverse
    |> Enum.take(n)
  end

  defp run_migrations([], dir, _state) do
    Logger.info("== Already #{dir}")
    {:ok, :already_applied}
  end
  defp run_migrations(to_apply, dir, state) do
    {time, res} = :timer.tc(&apply_migrations/3, [to_apply, dir, state])
    Logger.info("== Migrated in #{inspect(div(time, 10000)/10)}s")
    res
  end

  defp apply_migrations(to_apply, dir, state) do
    Enum.reduce_while(to_apply, {:ok, :applied},
      fn m, res ->
        case do_migrate(m, dir, state) do
          :ok -> {:cont, res}
          {:error, _} = err -> {:halt, err}
        end
      end)
  end

  defp do_migrate(migration = %Migration{filename: file}, direction, state) do
    :ok = Logger.info("== Migrating #{file} #{direction}")
    apply(migration.module, direction, [])
    update_db(migration, direction, state)
    :ok
  rescue
    error ->
      :ok = Logger.error(Exception.message(error))
      :ok = Logger.info("There was an error while trying to migrate #{file}")
      {:error, Exception.message(error)}
  end

  defp update_db(%Migration{authored_at: a, description: d}, :up, state) do
    insert into: state.table, in: state.keyspace,
      values: %{
        description: d,
        authored_at: a,
        applied_at: System.system_time(:milliseconds)
      }
  end
  defp update_db(%Migration{authored_at: a, description: d}, :down, state) do
    delete from: state.table, in: state.keyspace,
      where: %{authored_at: a, description: d}
  end
end
