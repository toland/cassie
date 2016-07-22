defmodule Mix.Schemata do
  @moduledoc false

  alias Schemata.Migrator

  def migrations_path do
    Application.load(:schemata)
    Application.fetch_env!(:schemata, :migrations_path)
  end

  def prepare do
    {:ok, _} = Application.ensure_all_started(:schemata)
    {:ok, _} = Migrator.start_link
    Migrator.load_migrations
  end

  def migrate(dir) do
    prepare
    Migrator.migrate(dir)
  end
end
