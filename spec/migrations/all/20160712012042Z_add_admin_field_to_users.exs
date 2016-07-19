defmodule Schemata.AddAdminFieldToUsersMigration do
  use Schemata.Migration, [
    authored_at: "2016-07-12T01:20:42Z",
    description: "Alter the user table and update users"
  ]

  def up do
    alter_table "users", in: "schemata_test",
      add: :admin, type: :boolean

    update "users", in: "schemata_test",
      set: %{admin: false}

    update "users", in: "schemata_test",
      set: %{admin: true},
      where: %{name: "bob"}
  end

  def down do
    alter_table "users", in: "schemata_test",
      drop: :admin
  end
end
