defmodule Mix.Tasks.Schemata.Rollback do
  @moduledoc false

  use Mix.Task
  import Mix.Schemata
  alias Schemata.Migrator

  def run(_args, migrator \\ &Migrator.run/3) do
    {:ok, _} = Application.ensure_all_started(:cqerl)
    opts = [log: true, n: 1]
    migrator.(migrations_path, :down, opts)
  end
end
