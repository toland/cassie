defmodule Schemata.CreateUsersTableMigration do
  use Schemata.Migration, [
    authored_at: "2016-07-11T23:00:07Z",
    description: "Create the users table and add users"
  ]

  def up do
    create_table "users", in: "schemata_test",
      columns: [
        name:       :text,
        email:      :text,
        created_at: :timestamp
      ],
      primary_key: [:name]

    insert into: "users", in: "schemata_test",
      values: %{name: "bob", email: "bob@company.com", created_at: :now}

    insert into: "users", in: "schemata_test",
      values: %{name: "fred", email: "fred@company.com", created_at: :now}

    insert into: "users", in: "schemata_test",
      values: %{name: "sue", email: "sue@company.com", created_at: :now}
  end

  def down do
    drop :table, named: "users", in: "schemata_test"
  end
end
