defmodule Schemata.Schemas.Wocky do
  use Schemata.Schema

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

  keyspace :complex_ks do
    table :table_with_views_and_indexes, [
      columns: [
        id: :text,
        name: :text
      ],
      primary_key: :id
    ]

    view :name_to_id, [
      from: :table_with_views_and_indexes,
      columns: :all,
      primary_key: [:name, :id]
    ]

    index [
      on: :table_with_views_and_indexes,
      keys: [:name]
    ]
  end
end
