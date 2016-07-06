# Schemata

Some simple Cassandra migration tasks for Mix.

# Usage
First we define our migrations. These are just ordinary CQL files in `priv/migrations` that are described via comments. For instance, we could want a table to store our import `foobar` data.

```cql
-- description: Creates the foobars table
-- authored_at: 1464076712285_create_foobars.cql
-- up:
USE test;

CREATE TABLE foobars (
  foo int,
  bar int,
  PRIMARY KEY (foo, bar)
);
-- down:
DROP TABLE foobars
```

Schemata will parse these CQL files for us, and let us view all migrations
with `schemata.migrations`.

```bash
$ mix schemata.migrations
Status   Name                                    Description
---------------------------------------------------------------------------------
up       1464076712285_create_foobars.cql         Creates the foobars table
up       1464076931287_create_users_table.cql    Adds qux to foobars
up       1464078503735_add_foobar_keyspace.cql   Adds the foobar keyspace
up       1464078511522_add_baz_table.cql         Makes the table baz
```

Let's rollback that last one with `schemata.rollback`.

```
$ mix schemata.rollback

01:32:32.203 [info]  == Running priv/migrations/1464078511522_add_baz_table.cql backwards

01:32:32.203 [info]  USE foobar;

01:32:32.204 [info]  DROP TABLE baz;

01:32:32.359 [info]  == Migrated in 1.5s
```

Listing them again shows the result of the rollback.

```bash
$ mix schemata.migrations
Status   Name                                    Description
---------------------------------------------------------------------------------
up       1464076712285_create_foobars.cql         Creates the foobars table
up       1464076931287_create_users_table.cql    Adds qux to foobars
up       1464078503735_add_foobar_keyspace.cql   Adds the foobar keyspace
down     1464078511522_add_baz_table.cql         Makes the table baz
```

So let's undo that and apply all pending migrations with `schemata.migrate`...

```
$ mix schemata.migrate

01:33:20.025 [info]  == Running priv/migrations/1464078511522_add_baz_table.cql

01:33:20.025 [info]  USE foobar;

01:33:20.026 [info]  CREATE TABLE baz

01:33:20.224 [info]  == Migrated in 1.0s
```

and show that they're all done again!

```bash
$ mix schemata.migrations
Status   Name                                    Description
---------------------------------------------------------------------------------
up       1464076712285_create_foobars.cql         Creates the foobars table
up       1464076931287_create_users_table.cql    Adds qux to foobars
up       1464078503735_add_foobar_keyspace.cql   Adds the foobar keyspace
up       1464078511522_add_baz_table.cql         Makes the table baz
```

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add schemata to your list of dependencies in `mix.exs`:

        def deps do
          [{:schemata, "~> 0.1.0"}]
        end

  2. Ensure schemata is started before your application:

        def application do
          [applications: [:schemata]]
        end
