defmodule Mix.Schemata do
  @moduledoc false

  alias Schemata.Migrator

  def migrations_path do
    Application.load(:schemata)
    Application.fetch_env!(:schemata, :migrations_path)
  end

  def start_schemata do
    {:ok, _} = Application.ensure_all_started(:schemata)
  end

  def migrate(dir) do
    start_schemata
    Migrator.migrate(dir)
  end
end
