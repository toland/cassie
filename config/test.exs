use Mix.Config

config :logger,
  backends: []

config :schemata,
  migrations_keyspace: "schemata_test",
  load_migrations_on_startup: false,
  load_schema_on_startup: false
