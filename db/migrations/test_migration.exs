defmodule TestMigration do
  use Schemata.Migration, [
    authored_at: ~N[2016-07-11 23:00:07],
    description: "Test migration description"
  ]

  def up do
    IO.write "migrate up"
  end

  # def down do
  #   IO.write "migrate down"
  # end
end
