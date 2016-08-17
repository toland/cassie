defmodule Schemata.QueryableSpec do
  use ESpec, shared: true

  alias Schemata.Queryable

  context "CQL statement generation for Queryables" do
    it "should generate the appropriate statement" do
      for {stmt, qry} <- shared.queries,
        do: qry |> Queryable.statement |> should(eq stmt)
    end
  end
end

defmodule Schemata.Queryable.SelectSpec do
  use ESpec

  alias Schemata.Query.Select

  before do
    {:shared, queries: [
      {"SELECT * FROM users",
       %Select{from: :users, values: :all}},
      {"SELECT * FROM users WHERE user = ?",
       %Select{from: :users, values: [], where: %{user: ""}}},
      {"SELECT * FROM users WHERE server = ? AND user = ?",
       %Select{from: :users, values: :all, where: %{user: "", server: ""}}},
      {"SELECT * FROM users WHERE server = ? LIMIT 1",
       %Select{from: :users, values: :all, where: %{server: ""}, limit: 1}},
      {"SELECT user FROM users",
       %Select{from: "users", values: [:user]}},
      {"SELECT user FROM users WHERE server = ?",
       %Select{from: "users", values: [:user], where: %{server: ""}}},
      {"SELECT user, server FROM users",
       %Select{from: "users", values: ["user", "server"]}},
      {"SELECT max(version) FROM users",
       %Select{from: "users", values: ['max(version)']}},
      {"SELECT COUNT(*) FROM users WHERE server = ?",
       %Select{from: "users", values: ["COUNT(*)"], where: %{server: ""}}}
    ]}
  end

  context "Select", do: it_behaves_like(Schemata.QueryableSpec)
end

defmodule Schemata.Queryable.InsertSpec do
  use ESpec

  alias Schemata.Query.Insert

  before do
    {:shared, queries: [
      {"INSERT INTO users (user) VALUES (?)",
       %Insert{into: :users, values: %{user: ""}}},
      {"INSERT INTO users (server, user) VALUES (?, ?)",
       %Insert{into: :users, values: %{user: "", server: ""}}},
      {"INSERT INTO users (user) VALUES (?) IF NOT EXISTS",
       %Insert{into: "users", values: %{user: ""}, unique: true}},
      {"INSERT INTO users (server, user) VALUES (?, ?) IF NOT EXISTS",
       %Insert{into: "users", values: %{user: "", server: ""}, unique: true}},
      {"INSERT INTO users (user) VALUES (?) USING TTL ?",
       %Insert{into: 'users', values: %{user: ""}, ttl: 1}},
      {"INSERT INTO users (user) VALUES (?) IF NOT EXISTS USING TTL ?",
       %Insert{into: 'users', values: %{user: ""}, ttl: 1, unique: true}}
    ]}
  end

  context "Insert", do: it_behaves_like(Schemata.QueryableSpec)
end

defmodule Schemata.Queryable.UpdateSpec do
  use ESpec

  alias Schemata.Query.Update

  before do
    {:shared, queries: [
      {"UPDATE users SET password = ?",
       %Update{table: :users, set: %{password: ""}}},
      {"UPDATE users SET handle = ?, password = ?",
       %Update{table: :users, set: %{password: "", handle: ""}}},
      {"UPDATE users SET password = ? WHERE user = ?",
       %Update{table: "users", set: %{password: ""}, where: %{user: ""}}},
      {"UPDATE users SET password = ? WHERE server = ? AND user = ?",
       %Update{table: "users", set: %{password: ""},
               where: %{user: "", server: ""}}},
      {"UPDATE users SET handle = ?, password = ? WHERE user = ?",
       %Update{table: 'users', set: %{password: "", handle: ""},
               where: %{user: ""}}},
      {"UPDATE users SET name = ?, password = ? WHERE server = ? AND user = ?",
       %Update{table: 'users', set: %{password: "", name: ""},
               where: %{user: "", server: ""}}}
    ]}
  end

  context "Update", do: it_behaves_like(Schemata.QueryableSpec)
end

defmodule Schemata.Queryable.DeleteSpec do
  use ESpec

  alias Schemata.Query.Delete

  before do
    {:shared, queries: [
      {"DELETE FROM users",
       %Delete{from: :users, values: :all}},
      {"DELETE FROM users",
       %Delete{from: :users, values: []}},
      {"DELETE FROM users WHERE user = ?",
       %Delete{from: :users, values: :all, where: %{user: ""}}},
      {"DELETE FROM users WHERE server = ? AND user = ?",
       %Delete{from: :users, values: :all, where: %{user: "", server: ""}}},
      {"DELETE server FROM users",
       %Delete{from: :users, values: [:server]}},
      {"DELETE user, server FROM users",
       %Delete{from: :users, values: [:user, :server]}},
      {"DELETE server FROM users WHERE user = ?",
       %Delete{from: :users, values: [:server], where: %{user: ""}}},
      {"DELETE server FROM users WHERE server = ? AND user = ?",
       %Delete{from: :users, values: [:server], where: %{user: "", server: ""}}}
    ]}
  end

  context "Delete", do: it_behaves_like(Schemata.QueryableSpec)
end

defmodule Schemata.Queryable.TruncateSpec do
  use ESpec

  alias Schemata.Query.Truncate

  before do
    {:shared, queries: [
      {"TRUNCATE TABLE users", %Truncate{table: :users}}
    ]}
  end

  context "Truncate", do: it_behaves_like(Schemata.QueryableSpec)
end

defmodule Schemata.Queryable.DropSpec do
  use ESpec

  alias Schemata.Query.Drop

  before do
    {:shared, queries: [
      {"DROP TABLE IF EXISTS users",
       %Drop{object: :table, named: :users}},
      {"DROP KEYSPACE IF EXISTS users",
       %Drop{object: :keyspace, named: :users}},
      {"DROP MATERIALIZED VIEW IF EXISTS users",
       %Drop{object: :materialized_view, named: :users}}
    ]}
  end

  context "Drop", do: it_behaves_like(Schemata.QueryableSpec)
end

defmodule Schemata.Queryable.CreateKeyspaceSpec do
  use ESpec

  alias Schemata.Query.CreateKeyspace

  before do
    ck = %CreateKeyspace{named: "test_ks"}
    {:shared, queries: [
      {"CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = " <>
       "{'class': 'SimpleStrategy', 'replication_factor': 1}",
       ck},
      {"CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = " <>
       "{'class': 'NetworkTopologyStrategy', 'dc1': 3}",
       %CreateKeyspace{ck | strategy: :network_topology, factor: [dc1: 3]}},
      {"CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = " <>
       "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 2}",
       %CreateKeyspace{ck | strategy: :network_topology,
                            factor: [dc1: 3, dc2: 2]}}
    ]}
  end

  context "Create Keyspace", do: it_behaves_like(Schemata.QueryableSpec)
end

defmodule Schemata.Queryable.CreateTableSpec do
  use ESpec

  alias Schemata.Query.CreateTable

  before do
    ct = %CreateTable{named: :test_tbl, columns: [id: :uuid], primary_key: :id}
    {:shared, queries: [
      {"CREATE TABLE IF NOT EXISTS test_tbl (id uuid, PRIMARY KEY (id))",
       ct},
      {"CREATE TABLE IF NOT EXISTS test_tbl (id uuid, PRIMARY KEY (id))",
       %CreateTable{ct | primary_key: [:id]}},
      {"CREATE TABLE IF NOT EXISTS test_tbl" <>
       " (id uuid, PRIMARY KEY (foo, bar, baz))",
       %CreateTable{ct | primary_key: [:foo, :bar, :baz]}},
      {"CREATE TABLE IF NOT EXISTS test_tbl" <>
       " (id uuid, PRIMARY KEY ((foo, bar), baz))",
       %CreateTable{ct | primary_key: [[:foo, :bar], :baz]}},
      {"CREATE TABLE IF NOT EXISTS test_tbl" <>
       " (id uuid, PRIMARY KEY ((foo, bar, baz)))",
       %CreateTable{ct | primary_key: [[:foo, :bar, :baz]]}},
      {"CREATE TABLE IF NOT EXISTS test_tbl" <>
       " (first text, second text, PRIMARY KEY (id))",
       %CreateTable{ct | columns: [first: :text, second: :text]}},
      {"CREATE TABLE IF NOT EXISTS test_tbl (id uuid, PRIMARY KEY (id))" <>
       " WITH CLUSTERING ORDER BY (foo ASC)",
       %CreateTable{ct | order_by: :foo}},
      {"CREATE TABLE IF NOT EXISTS test_tbl (id uuid, PRIMARY KEY (id))" <>
       " WITH CLUSTERING ORDER BY (foo ASC)",
       %CreateTable{ct | order_by: [foo: :asc]}},
      {"CREATE TABLE IF NOT EXISTS test_tbl (id uuid, PRIMARY KEY (id))" <>
       " WITH CLUSTERING ORDER BY (bar DESC)",
       %CreateTable{ct | order_by: [bar: :desc]}},
      {"CREATE TABLE IF NOT EXISTS test_tbl (id set<int>, PRIMARY KEY (id))",
       %CreateTable{ct | columns: [id: {:set, :int}]}},
      {"CREATE TABLE IF NOT EXISTS test_tbl (id list<int>, PRIMARY KEY (id))",
       %CreateTable{ct | columns: [id: {:list, :int}]}},
      {"CREATE TABLE IF NOT EXISTS test_tbl" <>
       " (id map<int,int>, PRIMARY KEY (id))",
       %CreateTable{ct | columns: [id: {:map, :int, :int}]}}
    ]}
  end

  context "Create Table", do: it_behaves_like(Schemata.QueryableSpec)
end

defmodule Schemata.Queryable.AlterTableSpec do
  use ESpec

  alias Schemata.Query.AlterTable

  before do
    {:shared, queries: [
      {"ALTER TABLE users ALTER email TYPE text",
       %AlterTable{named: "users", column: :email, op: {:alter, :text}}},
      {"ALTER TABLE users ADD email text",
       %AlterTable{named: "users", column: :email, op: {:add, :text}}},
      {"ALTER TABLE users RENAME email TO mail",
       %AlterTable{named: "users", column: :email, op: {:rename, :mail}}},
      {"ALTER TABLE users DROP email",
       %AlterTable{named: "users", column: :email, op: :drop}},
    ]}
  end

  context "Alter Table", do: it_behaves_like(Schemata.QueryableSpec)
end

defmodule Schemata.Queryable.CreateIndexSpec do
  use ESpec

  alias Schemata.Query.CreateIndex

  before do
    {:shared, queries: [
      {"CREATE INDEX IF NOT EXISTS ON users (user)",
       %CreateIndex{on: :users, keys: [:user]}},
      {"CREATE INDEX IF NOT EXISTS ON users (user, server)",
       %CreateIndex{on: "users", keys: ["user", "server"]}}
    ]}
  end

  context "Create Index", do: it_behaves_like(Schemata.QueryableSpec)
end

defmodule Schemata.Queryable.CreateViewSpec do
  use ESpec

  alias Schemata.Query.CreateView

  before do
    {:shared, queries: [
      {"""
       CREATE MATERIALIZED VIEW IF NOT EXISTS user_email AS\
        SELECT * FROM users\
        WHERE user IS NOT NULL\
        PRIMARY KEY (user)\
       """,
       %CreateView{
         named: :user_email, from: :users,
         columns: :all,
         primary_key: :user
       }
      },
      {"""
       CREATE MATERIALIZED VIEW IF NOT EXISTS user_email AS\
        SELECT * FROM users\
        WHERE user IS NOT NULL\
        AND email IS NOT NULL\
        PRIMARY KEY (user, email)\
        WITH CLUSTERING ORDER BY (email ASC)\
       """,
       %CreateView{
         named: :user_email, from: :users,
         columns: :all,
         primary_key: [:user, :email],
         order_by: [email: :asc]
       }
      },
      {"""
       CREATE MATERIALIZED VIEW IF NOT EXISTS user_email AS\
        SELECT * FROM users\
        WHERE user IS NOT NULL\
        AND created_at IS NOT NULL\
        AND email IS NOT NULL\
        PRIMARY KEY (user, created_at, email)\
       """,
       %CreateView{
         named: "user_email", from: "users",
         columns: :all,
         primary_key: [:user, :created_at, :email]
       }
      },
      {"""
       CREATE MATERIALIZED VIEW IF NOT EXISTS user_email AS\
        SELECT user, email FROM users\
        WHERE user IS NOT NULL\
        AND created_at IS NOT NULL\
        AND email IS NOT NULL\
        PRIMARY KEY (user, created_at, email)\
        WITH CLUSTERING ORDER BY (created_at ASC)\
       """,
       %CreateView{
         named: :user_email, from: :users,
         columns: [:user, :email],
         primary_key: [:user, :created_at, :email],
         order_by: [created_at: :asc]
       }
      }
    ]}
  end

  context "Create View", do: it_behaves_like(Schemata.QueryableSpec)
end
