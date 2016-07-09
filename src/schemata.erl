-module(schemata).

-export([select/4]).


select(Keyspace, Table, Columns, Conditions) ->
    'Elixir.Schemata':select(Keyspace, Table, Columns, Conditions).
