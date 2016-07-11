defmodule Schemata.CQErl do
  @moduledoc false

  defmacro __using__(_opts) do
    import Schemata.CQErl
    alias :cqerl, as: CQErl
  end

  require Record
  import Record, only: [defrecordp: 2, extract: 2]

  defrecordp :cql_query, extract(:cql_query, from_lib: "cqerl/include/cqerl.hrl")
  defrecordp :cql_result, extract(:cql_result, from_lib: "cqerl/include/cqerl.hrl")
end
