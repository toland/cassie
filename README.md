# Schemata

Some simple Cassandra migration tasks for Mix.

## Migrations
First we define our migrations. These are just ordinary Elixir files in `priv/migrations` that have some added metadata. For instance, we could want a table to store our import `foobar` data.

```elixir
defmodule Schemata.CreateFoobarsMigration do
  use Schemata.Migration, [
    authored_at: "2016-07-22T03:28:53Z",
    description: "Create the foobars table"
  ]

  def up do
    create_table "foobars", in: "test",
      columns: [
        foo: :text,
        bar: :text
      ],
      primary_key: [:foo, :bar]
  end

  def down do
    drop :table, named: "foobars", in: "test"
  end
end
```

Schemata will load these Elixir files for us, and let us view all migrations
with `schemata.migrations`.

```bash
$ mix schemata.migrations
Status   Name                                    Description
---------------------------------------------------------------------------------
up       1464076712285_create_foobars.exs        Creates the foobars table
up       1464076931287_create_users_table.exs    Adds qux to foobars
up       1464078503735_add_foobar_keyspace.exs   Adds the foobar keyspace
up       1464078511522_add_baz_table.exs         Makes the table baz
```

Let's rollback that last one with `schemata.rollback`.

```
$ mix schemata.rollback

01:32:32.203 [info]  == Migrating priv/migrations/1464078511522_add_baz_table.exs down

01:32:32.359 [info]  == Migrated in 1.5s
```

Listing them again shows the result of the rollback.

```bash
$ mix schemata.migrations
Status   Name                                    Description
---------------------------------------------------------------------------------
up       1464076712285_create_foobars.exs        Creates the foobars table
up       1464076931287_create_users_table.exs    Adds qux to foobars
up       1464078503735_add_foobar_keyspace.exs   Adds the foobar keyspace
down     1464078511522_add_baz_table.exs         Makes the table baz
```

So let's undo that and apply all pending migrations with `schemata.migrate`...

```
$ mix schemata.migrate

01:33:20.025 [info]  == Migrating priv/migrations/1464078511522_add_baz_table.exs up

01:33:20.224 [info]  == Migrated in 1.0s
```

and show that they're all done again!

```bash
$ mix schemata.migrations
Status   Name                                    Description
---------------------------------------------------------------------------------
up       1464076712285_create_foobars.exs        Creates the foobars table
up       1464076931287_create_users_table.exs    Adds qux to foobars
up       1464078503735_add_foobar_keyspace.exs   Adds the foobar keyspace
up       1464078511522_add_baz_table.exs         Makes the table baz
```

## Installation

  1. Add schemata to your list of dependencies in `mix.exs`:

        def deps do
          [{:schemata, "~> 0.1.0"}]
        end

  2. Ensure schemata is started before your application:

        def application do
          [applications: [:schemata]]
        end
