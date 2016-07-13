defmodule Mix.Tasks.Schemata.Migrate do
  use Mix.Task
  import Mix.Schemata

  def run(_args, migrator \\ &Schemata.Migrator.run/3) do
    {:ok, _} = Application.ensure_all_started(:cqerl)
    opts = [log: true]
    migrator.(migrations_path, :up, opts)
  end
end
