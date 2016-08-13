defmodule Schemata.Schema do
  @moduledoc ""

  use GenServer
  alias Schemata.Query
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

  @spec schema_file :: binary
  def schema_file do
    GenServer.call(SchemaServer, :schema_file)
  end

  @spec load_schema(binary | nil) :: :ok | {:error, term}
  def load_schema(file \\ nil) do
    GenServer.call(SchemaServer, {:load_schema, file})
  end

  @spec list_tables(Query.keyspace) :: [Query.table]
  def list_tables(keyspace) do
    GenServer.call(SchemaServer, {:list_tables, keyspace})
  end

  @spec apply_schema(Query.keyspace | nil) :: {:ok, :applied} | {:error, term}
  def apply_schema(keyspace \\ nil) do
    GenServer.call(SchemaServer, {:apply_schema, keyspace})
  end

  @spec create_table(Query.keyspace, Query.table) :: :ok | {:error, term}
  def create_table(keyspace, table) do
    GenServer.call(SchemaServer, {:create_table, keyspace, to_atom(table)})
  end

  @spec create_table!(Query.keyspace, Query.table) :: :ok | {:error, term}
  def create_table!(keyspace, table) do
    GenServer.call(SchemaServer, {:recreate_table, keyspace, to_atom(table)})
  end


  # -------------------------------------------------------------------------
  # GenServer callbacks

  def init(_args) do
    file = Application.fetch_env!(:schemata, :schema_file)
    load? = Application.fetch_env!(:schemata, :load_schema_on_startup)

    if File.exists?(file) && load?, do: do_load_schema(file)

    {:ok, %State{schema_file: file}}
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

  def handle_call({:list_tables, keyspace}, _from, state) do
    tables = find_keyspace_tables(state.keyspace_tables, keyspace)
    {:reply, tables, state}
  end

  def handle_call({:apply_schema, keyspace}, _from, state) do
    state.keyspace_tables
    |> find_keyspace_tables(keyspace)
    |> Enum.each(&create_table(keyspace, &1, state))

    {:reply, {:ok, :applied}, state}
  end

  def handle_call({:recreate_table, keyspace, table}, _from, state) do
    result = recreate_table(keyspace, table, state)
    {:reply, result, state}
  end

  def handle_call({:create_table, keyspace, table}, _from, state) do
    result = create_table(keyspace, table, state)
    {:reply, result, state}
  end

  def handle_cast({:push_keyspace, pattern}, state) do
    {:noreply, %State{state | current_ks: pattern}}
  end

  def handle_cast({:push_table, name, struct}, state) do
    name = to_atom(name)

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
      table_views: push_value(state.table_views, to_atom(table), struct)}}
  end

  def handle_cast({:push_index, table, struct}, state) do
    {:noreply, %State{state |
      table_indexes: push_value(state.table_indexes, to_atom(table), struct)}}
  end


  # -------------------------------------------------------------------------
  # Private helper functions

  defp do_load_schema(file) do
    {module, _} = file |> Code.load_file |> hd
    flush(file, module)
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

  defp find_keyspace_tables(ks_tables, keyspace) do
    {_, value} = Enum.find(ks_tables, {nil, []}, &match_keyspace(&1, keyspace))
    value
  end

  defp match_keyspace({key, _}, keyspace) when is_atom(key) or is_binary(key),
    do: to_string(key) === to_string(keyspace)
  defp match_keyspace({key, _}, keyspace) do
    if Regex.regex?(key), do: Regex.match?(key, to_string(keyspace))
  end

  defp recreate_table(keyspace, table, state) do
    try do
      _ = state.table_views |> Map.get(table, []) |> drop_table_views(keyspace)

      :ok = Schemata.drop(:table, named: table, in: keyspace)
      create_table(keyspace, table, state)
    rescue
      error in CassandraError ->
        {:error, {error.code, error.message}}
    end
  end

  defp create_table(keyspace, table, state) do
    table_def = state.table_defs[table]

    try do
      :void = Query.run!(%CreateTable{table_def | in: keyspace})

      _ = state.table_indexes
          |> Map.get(table, [])
          |> create_table_indexes(keyspace)

      _ = state.table_views
          |> Map.get(table, [])
          |> create_table_views(keyspace)

      :ok
    rescue
      error in CassandraError ->
        {:error, {error.code, error.message}}
    end
  end

  defp drop_table_views(views, keyspace) do
    for %CreateView{named: name} <- views,
      do: Schemata.drop :materialized_view, named: name, in: keyspace
  end

  defp create_table_views(views, keyspace) do
    for view <- views, do: Query.run!(%CreateView{view | in: keyspace})
  end

  defp create_table_indexes(indexes, keyspace) do
    for index <- indexes, do: Query.run!(%CreateIndex{index | in: keyspace})
  end
end
