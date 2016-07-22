defmodule Mix.Tasks.Schemata.Rollback do
  @moduledoc false

  use Mix.Task
  import Mix.Schemata

  def run(_args) do
    migrate :down
  end
end
