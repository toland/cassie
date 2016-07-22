defmodule Mix.Tasks.Schemata.Migrate do
  @moduledoc false

  use Mix.Task
  import Mix.Schemata

  def run(_args) do
    migrate :up
  end
end
