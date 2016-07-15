defmodule TestMigration do
  use Schemata.Migration, [
    authored_at: ~N[2016-07-11 23:00:07],
    description: "Test migration description"
  ]

  def up do
    create_table "users", in: "schemata_test",
      columns: [
        user_id:    :text,
        email:      :text,
        created_at: :timestamp
      ],
      primary_key: [:user_id]
  end

  def down do
    drop :table, "users", in: "schemata_test"
  end
end
