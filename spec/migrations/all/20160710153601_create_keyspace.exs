defmodule Schemata.CreateKeyspaceMigration do
  use Schemata.Migration, [
    authored_at: "2016-07-10T15:36:01Z",
    description: "Create the schemata_test keyspace"
  ]

  def up do
    create_keyspace :schemata_test
  end

  def down do
    raise Schemata.MigrationError, message: "Cannot rollback"
  end
end
