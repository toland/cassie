defmodule Schemata.CQErl do
  @moduledoc false

  require Record
  import Record, only: [defrecord: 2, extract: 2]

  defrecord :cql_query,
    extract(:cql_query, from_lib: "cqerl/include/cqerl.hrl")

  defrecord :cql_query_batch,
    extract(:cql_query_batch, from_lib: "cqerl/include/cqerl.hrl")

  defrecord :cql_result,
    extract(:cql_result, from_lib: "cqerl/include/cqerl.hrl")

  defmacro __using__(_opts) do
    quote do
      import Schemata.CQErl
      alias :cqerl, as: CQErl
    end
  end
end
