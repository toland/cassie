-module(schemata).

-export([select/4]).


select(Keyspace, Table, Columns, Conditions) ->
    'Elixir.Schemata':select(Columns, [
        {from, Table},
        {in, Keyspace},
        {where, Conditions}
    ]).
