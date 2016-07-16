defmodule Schemata do
  @moduledoc ""

  alias Schemata.Query

  defmodule CassandraError do
    @moduledoc ""

    defexception [
      error_message: nil,
      error_code:    nil,
      query:         nil
    ]

    def message(%__MODULE__{error_message: message, error_code: code}) do
      "Error Code #{code}: #{message}"
    end
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
  @spec select(Query.columns, Keyword.t) :: Query.rows
  def select(columns, query) do
    %{query | values: columns}
    |> Schemata.Query.Select.from_map
    |> Query.run!
    |> Query.all_rows
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
    query
    |> Schemata.Query.Insert.from_map
    |> Query.run!
    |> Query.single_result
  end

  @doc """
  Updates rows in a table with the provided values.

    update "users", in: "my_db",
      set: %{email: "bob@company.com"}
      where: %{user_name: "bob"}
  """
  @spec update(Query.table, Keyword.t) :: :ok
  def update(table, query) do
    %{query | table: table}
    |> Schemata.Query.Update.from_map
    |> Query.run!
    :ok
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
    |> Schemata.Query.Delete.from_map
    |> Query.run!
    :ok
  end

  @doc """
  Truncates a table.

    truncate table: "users", in: "my_db"
  """
  @spec truncate(Keyword.t) :: :ok
  def truncate(query) do
    query
    |> Schemata.Query.Truncate.from_map
    |> Query.run!
    :ok
  end

  @doc """
  Drops a database object

    drop :table, named: "users"
  """
  @spec drop(atom, Keyword.t) :: :ok
  def drop(object, query) do
    %{query | object: object}
    |> Schemata.Query.Drop.from_map
    |> Query.run!
    :ok
  end

  @doc """
  Creates a keyspace.

    create_keyspace :my_ks,
      strategy: :simple,
      factor: 1
  """
  @spec create_keyspace(Query.keyspace, map) :: :ok
  def create_keyspace(name, query) do
    %{query | named: name}
    |> Schemata.Query.CreateKeyspace.from_map
    |> Query.run!
    :ok
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
  @spec create_table(Query.table, map) :: :ok
  def create_table(name, query) do
    %{query | named: name}
    |> Schemata.Query.CreateTable.from_map
    |> Query.run!
    :ok
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
  @spec alter_table(Query.table, map) :: :ok
  def alter_table(name, query) do
    %{query | named: name}
    |> Schemata.Query.AlterTable.from_map
    |> Query.run!
    :ok
  end

  @doc """
  Creates an index.

    create_index on: :users, keys: [:user, :email]
  """
  @spec create_index(map) :: :ok
  def create_index(query) do
    query
    |> Schemata.Query.CreateIndex.from_map
    |> Query.run!
    :ok
  end

  @doc """
  Creates a materialized view.

    create_view "my_view",
      from: "my_table", in: "my_db",
      columns: [:a, :b, :c],
      primary_key: [:b],
      order_by: [b: :asc]
  """
  @spec create_view(Query.table, map) :: :ok
  def create_view(name, query) do
    %{query | named: name}
    |> Schemata.Query.CreateView.from_map
    |> Query.run!
    :ok
  end
end
