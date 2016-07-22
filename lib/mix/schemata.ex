defmodule Mix.Schemata do
  @moduledoc false

  alias Schemata.Migrator

  def migrations_path do
    :ok = Application.load(:schemata)
    Application.fetch_env!(:schemata, :migrations_path)
  end

  def start_schemata do
    {:ok, _} = Application.ensure_all_started(:schemata)
    :ok
  end

  def migrate(dir) do
    :ok = start_schemata
    Migrator.migrate(dir)
  end
end
