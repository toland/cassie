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

  keyspace :ks_atom do
    table :ks_atom_table, [
      columns: [
        id: :text
      ],
      primary_key: :id
    ]
  end

  keyspace "ks_binary" do
    table "ks_binary_table", [
      columns: [
        id: :text
      ],
      primary_key: :id
    ]
  end

  keyspace ~r/ks_(test_)?(regex|.*_foo)/ do
    table :ks_regex_table, [
      columns: [
        id: :text
      ],
      primary_key: :id
    ]
  end
end
