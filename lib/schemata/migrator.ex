defmodule Schemata.Migrator do
  @moduledoc ""

  use Timex
  use GenServer
  use Schemata.CQErl
  import Schemata
  alias Schemata.Migration
  require Logger

  defmodule State do
    @moduledoc false
    defstruct [
      migrations_dir: nil,
      keyspace:       nil,
      table:          nil,
      migrations:     []
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

  @spec flush() :: :ok
  def flush do
    GenServer.call(Migrator, :flush)
  end

  @spec migrations_dir :: binary
  def migrations_dir do
    GenServer.call(Migrator, :migrations_dir)
  end

  @spec load_migrations(binary | nil) :: :ok | {:error, term}
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

  def init(_args) do
    state = %State{
      migrations_dir: Application.fetch_env!(:schemata, :migrations_dir),
      keyspace: Application.fetch_env!(:schemata, :migrations_keyspace),
      table: Application.fetch_env!(:schemata, :migrations_table)
    }

    ensure_migrations_table!(state.keyspace, state.table)

    :schemata
    |> Application.fetch_env!(:load_migrations_on_startup)
    |> maybe_load_migrations(state)
  end

  def handle_call(:migrations_dir, _from, state) do
    {:reply, state.migrations_dir, state}
  end

  def handle_call({:load_migrations, nil}, from, state) do
    handle_call({:load_migrations, state.migrations_dir}, from, state)
  end

  def handle_call({:load_migrations, dir}, _from, state) do
    migrations = load_migrations(dir, state.keyspace, state.table)
    {:reply, :ok, %State{state | migrations_dir: dir, migrations: migrations}}
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
    result =
      state.migrations
      |> applicable_migrations(dir, n)
      |> run_migrations(dir, state)

    migrations =
      state.keyspace
      |> load_migrations_from_db(state.table)
      |> merge_migrations(state.migrations)

    {:reply, result, %State{state | migrations: migrations}}
  end

  def handle_call(:flush, _from, state) do
    :ok = flush(state.migrations)
    {:reply, :ok, state}
  end

  def terminate(_reason, %State{migrations: migrations}) do
    flush(migrations)
  end


  # -------------------------------------------------------------------------
  # Private helper functions

  defp ensure_migrations_table!(keyspace, table) do
    create_keyspace keyspace
    create_table "#{keyspace}.#{table}",
      columns: [
        authored_at: :timestamp,
        description: :text,
        applied_at:  :timestamp
      ],
      primary_key: [:authored_at, :description]
  end

  defp maybe_load_migrations(false, state), do: {:ok, state}
  defp maybe_load_migrations(true, state) do
    dir = state.migrations_dir
    if File.exists?(dir) do
      migrations = load_migrations(dir, state.keyspace, state.table)
      {:ok, %State{state | migrations: migrations}}
    else
      {:ok, state}
    end
  end

  defp load_migrations(dir, keyspace, table) do
    all = load_migrations_from_files(dir)

    keyspace
    |> load_migrations_from_db(table)
    |> merge_migrations(all)
  end

  defp load_migrations_from_files(dir) do
    dir
    |> File.ls!
    |> Enum.map(fn file -> dir |> Path.join(file) |> Migration.load_file end)
  end

  defp load_migrations_from_db(keyspace, table) do
    :all
    |> select(from: table, in: keyspace)
    |> Enum.map(&Migration.from_map/1)
  end

  defp merge_migrations(applied, all) do
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
    migrations
    |> applied_migrations
    |> Enum.reverse
    |> Enum.take(n)
  end

  defp run_migrations([], dir, _state) do
    :ok = Logger.info("== Already #{dir}")
    {:ok, :already_applied}
  end
  defp run_migrations(to_apply, dir, state) do
    {time, res} = :timer.tc(&apply_migrations/3, [to_apply, dir, state])
    :ok = Logger.info("== Migrated in #{inspect(div(time, 10000)/10)}s")
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
    insert into: "#{state.keyspace}.#{state.table}",
      values: %{
        description: d,
        authored_at: a,
        applied_at: System.system_time(:milliseconds)
      }
  end
  defp update_db(%Migration{authored_at: a, description: d}, :down, state) do
    delete from: "#{state.keyspace}.#{state.table}",
      where: %{authored_at: a, description: d}
  end

  defp flush(migrations) do
    Code.unload_files(Code.loaded_files)
    for m <- migrations do
      :code.purge(m.module)
      :code.delete(m.module)
      :code.purge(m.module)
    end
    :ok
  end
end
