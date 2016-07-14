defmodule Schemata.Query.SelectSpec do
  use ESpec

  alias Schemata.Query.Select
  alias Schemata.Queryable

  context "CQL statement generation for SELECT queryies" do
    let :queries, do: [
      {"SELECT * FROM users",
       %Select{from: :users, values: :all}},
      {"SELECT * FROM users WHERE user = ?",
       %Select{from: :users, values: [], where: %{user: ""}}},
      {"SELECT * FROM users WHERE server = ? AND user = ?",
       %Select{from: :users, values: :all, where: %{user: "", server: ""}}},
      {"SELECT * FROM users WHERE server = ? AND user = ? LIMIT 1",
       %Select{from: :users, values: :all, where: %{user: "", server: ""}, limit: 1}},
      {"SELECT user FROM users",
       %Select{from: "users", values: [:user]}},
      {"SELECT user FROM users WHERE server = ?",
       %Select{from: "users", values: [:user], where: %{server: ""}}},
      {"SELECT user, server FROM users",
       %Select{from: "users", values: ["user", "server"]}},
      {"SELECT max(version) FROM users",
       %Select{from: "users", values: ['max(version)']}}
    ]

    it "should generate the appropriate statement from each struct" do
      for {stmt, qry} <- queries, do: Queryable.statement(qry) |> should(eq stmt)
    end
  end
end
