defmodule Schemata.Schemas.Wocky do
  use Schemata.Schema

  keyspace :schemata_test do
    table :test_table, [
      columns: [
        id: :text,
        data: :text
      ],
      primary_key: :id
    ]

    index on: :test_table, keys: [:data]

    view :test_view, [
      from: :test_table,
      columns: :all,
      primary_key: [:data, :id]
    ]
  end
end
