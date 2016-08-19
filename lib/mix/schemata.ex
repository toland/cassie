defmodule Mix.Schemata do
  @moduledoc false

  alias Schemata.Migrator

  def migrations_path do
    case Application.load(:schemata) do
      {:error, {:already_loaded, :schemata}} -> :ok
      :ok -> :ok
    end
    Application.fetch_env!(:schemata, :migrations_dir)
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
