defmodule Schemata.AddAdminFieldToUsersMigration do
  use Schemata.Migration, [
    authored_at: "2016-07-12T01:20:42Z",
    description: "Alter the user table and update users"
  ]

  def up do
    alter_table "users", in: "schemata_test",
      add: :admin, type: :boolean

    users = select :all, from: "users", in: "schemata_test"

    for user <- users do
      case user.name do
        "bob" ->
          update "users", in: "schemata_test",
            set: %{admin: true},
            where: %{name: "bob"}

        name ->
          update "users", in: "schemata_test",
            set: %{admin: false},
            where: %{name: name}
      end
    end
  end

  def down do
    alter_table "users", in: "schemata_test",
      drop: :admin
  end
end
