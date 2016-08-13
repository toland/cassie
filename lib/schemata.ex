defmodule Schemata do
  @moduledoc ""

  alias Schemata.Query
  alias Schemata.Result

  defmodule CassandraError do
    @moduledoc ""

    defexception [:message, :code, :keyspace, :query, :stack]

    def message(%__MODULE__{message: message, code: nil}),
      do: message

    def message(%__MODULE__{message: message, code: code}),
      do: "Error Code #{code}: #{message}"

    def exception(message) when is_bitstring(message),
      do: %__MODULE__{message: message}

    def exception(args),
      do: %__MODULE__{
        message:  to_string(args[:message]),
        code:     args[:code],
        keyspace: args[:keyspace],
        query:    args[:query],
        stack:    args[:stack]
      }
  end

  defmodule MigrationError do
    @moduledoc ""

    defexception [message: nil]
  end

  @doc """
  Retrieves data from a table based on the parameters and returns all rows
  of the result set.

    select :all,
      from: "my_db.users",
      where: %{user_id: "bob"},
      limit: 1

    select :all,
      from: "users", in: "my_db",
      where: %{user_id: "bob"},
      limit: 1
      with: :quorum
  """
  @spec select(Query.columns, Keyword.t) :: Result.rows
  def select(columns, query) do
    query
    |> Keyword.put(:values, columns)
    |> Schemata.Query.Select.from_opts
    |> Query.run!
    |> Result.all_rows
  end

  @doc """
  Retrieves data from a table based on the parameters and returns all rows
  of the result set.

    count "users", in: "my_db",
      where: %{admin: true},
      with: :quorum
  """
  @spec count(Query.table, Keyword.t) :: non_neg_integer
  def count(table, query) do
    query
    |> Keyword.put(:from, table)
    |> Keyword.put(:values, ["COUNT(*)"])
    |> Schemata.Query.Select.from_opts
    |> Query.run!
    |> Result.single_value
  end

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
    result =
      query
      |> Schemata.Query.Insert.from_opts
      |> Query.run!

    case result do
      :void -> true
      _else -> Result.single_value(result)
    end
  end

  @doc """
  Updates rows in a table with the provided values.

    update "users", in: "my_db",
      set: %{email: "bob@company.com"}
      where: %{user_name: "bob"}
  """
  @spec update(Query.table, Keyword.t) :: :ok
  def update(table, query) do
    query
    |> Keyword.put(:table, table)
    |> Schemata.Query.Update.from_opts
    |> Query.run!
    |> ignore_result
  end

  @doc """
  Deletes rows from a table.

    delete from: "users", in: "my_db",
      values: [:email],
      where: %{user_name: "bob"}
  """
  @spec delete(Keyword.t) :: :ok
  def delete(query) do
    query
    |> Schemata.Query.Delete.from_opts
    |> Query.run!
    |> ignore_result
  end

  @doc """
  Truncates a table.

    truncate table: "users", in: "my_db"
  """
  @spec truncate(Keyword.t) :: :ok
  def truncate(query) do
    query
    |> Schemata.Query.Truncate.from_opts
    |> Query.run!
    |> ignore_result
  end

  @doc """
  Drops a database object

    drop :table, named: "users"
  """
  @spec drop(atom, Keyword.t) :: :ok
  def drop(object, query) do
    object = if object == :view, do: :materialized_view, else: object

    query
    |> Keyword.put(:object, object)
    |> Schemata.Query.Drop.from_opts
    |> Query.run!
    |> ignore_result
  end

  @doc """
  Creates a keyspace.

    create_keyspace :my_ks,
      strategy: :simple,
      factor: 1
  """
  @spec create_keyspace(Query.keyspace, Keyword.t) :: :ok
  def create_keyspace(name, query \\ []) do
    query
    |> Keyword.put(:named, name)
    |> Schemata.Query.CreateKeyspace.from_opts
    |> Query.run!
    |> ignore_result
  end

  @doc """
  Creates a table.

    create_table :users, in: "my_keyspace",
      columns: [
        user_id: :text,
        email: :text,
        created_at: :timestamp
      ],
      primary_key: [:user_id, :email]
      order_by: [created_at: :desc]
  """
  @spec create_table(Query.table, Keyword.t) :: :ok
  def create_table(name, query) do
    query
    |> Keyword.put(:named, name)
    |> Schemata.Query.CreateTable.from_opts
    |> Query.run!
    |> ignore_result
  end

  @doc """
  Alters an existing table.

    alter_table :users, in: "my_keyspace",
      alter: :email, type: :text

    alter_table :users, in: "my_keyspace",
      add: :email, type: :text

    alter_table :users, in: "my_keyspace",
      drop: :email

    alter_table :users, in: "my_keyspace",
      rename: :email, to: :email_address
  """
  @spec alter_table(Query.table, Keyword.t) :: :ok
  def alter_table(name, query) do
    query
    |> Keyword.put(:named, name)
    |> Schemata.Query.AlterTable.from_opts
    |> Query.run!
    |> ignore_result
  end

  @doc """
  Creates an index.

    create_index on: :users, keys: [:user, :email]
  """
  @spec create_index(Keyword.t) :: :ok
  def create_index(query) do
    query
    |> Schemata.Query.CreateIndex.from_opts
    |> Query.run!
    |> ignore_result
  end

  @doc """
  Creates a materialized view.

    create_view "my_view",
      from: "my_table", in: "my_db",
      columns: [:a, :b, :c],
      primary_key: [:b],
      order_by: [b: :asc]
  """
  @spec create_view(Query.table, Keyword.t) :: :ok
  def create_view(name, query) do
    query
    |> Keyword.put(:named, name)
    |> Schemata.Query.CreateView.from_opts
    |> Query.run!
    |> ignore_result
  end

  defp ignore_result(_result), do: :ok
end
