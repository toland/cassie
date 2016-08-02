defmodule Schemata.Schema do
  @moduledoc ""

  use GenServer
  alias Schemata.Query
  alias Schemata.Queryable
  alias Schemata.Query.CreateTable
  alias Schemata.Query.CreateIndex
  alias Schemata.Query.CreateView
  require Logger

  defmodule State do
    @moduledoc false
    defstruct [
      schema_file:     nil,
      current_ks:      nil,
      keyspace_tables: %{},
      table_defs:      %{},
      table_indexes:   %{},
      table_views:     %{}
    ]
  end


  # -------------------------------------------------------------------------
  # Schema DSL

  defmacro __using__(_args) do
    quote do
      import Schemata.Schema, only: [keyspace: 2, table: 2, view: 2, index: 1]
    end
  end

  defmacro keyspace(pattern, [do: block]) do
    quote do
      GenServer.cast(SchemaServer, {:push_keyspace, unquote(pattern)})
      unquote(block)
    end
  end

  def table(name, opts) do
    struct =
      opts
      |> Keyword.put(:named, name)
      |> Schemata.Query.CreateTable.from_opts

    GenServer.cast(SchemaServer, {:push_table, to_atom(name), struct})
  end

  def view(name, opts) do
    struct =
      opts
      |> Keyword.put(:named, name)
      |> Schemata.Query.CreateView.from_opts

    GenServer.cast(SchemaServer, {:push_view, to_atom(struct.from), struct})
  end

  def index(opts) do
    struct = opts |> Schemata.Query.CreateIndex.from_opts
    GenServer.cast(SchemaServer, {:push_index, to_atom(struct.on), struct})
  end


  # -------------------------------------------------------------------------
  # API

  @spec start_link(Keyword.t) :: {:ok, pid} | {:error, term}
  def start_link(args \\ []) do
    GenServer.start_link(__MODULE__, args, name: SchemaServer)
  end

  @spec stop() :: :ok
  def stop do
    GenServer.stop(SchemaServer, :normal)
  end

  @spec schema_file :: binary
  def schema_file do
    GenServer.call(SchemaServer, :schema_file)
  end

  @spec load_schema(binary | nil) :: :ok | {:error, term}
  def load_schema(file \\ nil) do
    GenServer.call(SchemaServer, {:load_schema, file})
  end

  @spec list_schema(Query.keyspace | nil) :: [Queryable.t]
  def list_schema(keyspace \\ nil) do
    GenServer.call(SchemaServer, {:list_schema, keyspace})
  end

  @spec apply_schema(Query.keyspace | nil) :: {:ok, :applied} | {:error, term}
  def apply_schema(keyspace \\ nil) do
    GenServer.call(SchemaServer, {:apply_schema, keyspace})
  end

  @spec create_table(Query.keyspace, Query.table) :: :ok | {:error, term}
  def create_table(keyspace, table) do
    GenServer.call(SchemaServer, {:create_table, keyspace, table})
  end

  @spec create_table!(Query.keyspace, Query.table) :: :ok | {:error, term}
  def create_table!(keyspace, table) do
    GenServer.call(SchemaServer, {:recreate_table, keyspace, table})
  end


  # -------------------------------------------------------------------------
  # GenServer callbacks

  def init(_args) do
    schema_file = Application.fetch_env!(:schemata, :schema_file)
    load? = Application.fetch_env!(:schemata, :load_schema_on_startup)

    if File.exists?(schema_file) && load?, do: do_load_schema(schema_file)

    {:ok, %State{schema_file: schema_file}}
  end

  def handle_call(:schema_file, _from, state) do
    {:reply, state.schema_file, state}
  end

  def handle_call({:load_schema, nil}, from, state) do
    handle_call({:load_schema, state.schema_file}, from, state)
  end

  def handle_call({:load_schema, file}, _from, state) do
    if File.exists?(file) do
      :ok = do_load_schema(file)
      {:reply, :ok, %State{state | schema_file: file}}
    else
      {:reply, {:error, :file_not_found}, state}
    end
  end

  def handle_call({:list_schema, nil}, _from, state) do
    IO.inspect state
    {:reply, :ok, state}
  end

  def handle_call({:list_schema, _keyspace}, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:apply_schema, nil}, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:apply_schema, _keyspace}, _from, state) do
    {:reply, :ok, state}
  end

  def handle_call({:recreate_table, keyspace, table}, from, state) do
    views = state.table_views[to_atom(table)]
    drop_table_views(keyspace, views)
    :ok = Schemata.drop(:table, named: table, in: keyspace)
    handle_call({:create_table, keyspace, table}, from, state)
  end

  def handle_call({:create_table, keyspace, table}, _from, state) do
    table = to_atom(table)

    table_def = state.table_defs[table]
    Query.run(%CreateTable{table_def | in: keyspace})

    create_table_indexes(keyspace, state.table_indexes[table])
    create_table_views(keyspace, state.table_views[table])

    {:reply, :ok, state}
  end

  def handle_cast({:push_keyspace, pattern}, state) do
    {:noreply, %State{state | current_ks: pattern}}
  end

  def handle_cast({:push_table, name, struct}, state) do
    %State{
      current_ks: current_ks,
      keyspace_tables: ks_tables,
      table_defs: table_defs
    } = state

    {:noreply, %State{state |
      keyspace_tables: push_value(ks_tables, current_ks, name),
      table_defs: Map.put(table_defs, name, struct)}}
  end

  def handle_cast({:push_view, table, struct}, state) do
    {:noreply, %State{state |
      table_views: push_value(state.table_views, table, struct)}}
  end

  def handle_cast({:push_index, table, struct}, state) do
    {:noreply, %State{state |
      table_indexes: push_value(state.table_indexes, table, struct)}}
  end


  # -------------------------------------------------------------------------
  # Private helper functions

  defp do_load_schema(schema_file) do
    {module, _} = schema_file |> Code.load_file |> hd
    flush(schema_file, module)
    :ok
  end

  defp push_value(map, key, value) do
    Map.update(map, key, [value], &([value | &1]))
  end

  defp to_atom(string) when is_binary(string), do: String.to_atom(string)
  defp to_atom(atom) when is_atom(atom), do: atom

  defp flush(file, module) do
    Code.unload_files([file])
    :code.purge(module)
    :code.delete(module)
    :code.purge(module)
    :ok
  end

  defp drop_table_views(keyspace, views) do
    for %CreateView{named: name} <- views,
      do: Schemata.drop :materialized_view, named: name, in: keyspace
  end

  defp create_table_views(keyspace, views) do
    for view <- views, do: Query.run(%CreateView{view | in: keyspace})
  end

  defp create_table_indexes(keyspace, indexes) do
    for index <- indexes, do: Query.run(%CreateIndex{index | in: keyspace})
  end
end
