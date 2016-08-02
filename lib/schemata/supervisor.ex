defmodule Schemata.Supervisor do
  @moduledoc ""

  use Supervisor

  def start_link do
    Supervisor.start_link(__MODULE__, [])
  end

  def init([]) do
    children = [
      worker(Schemata.Migrator, []),
      worker(Schemata.Schema, [])
    ]

    supervise(children, strategy: :one_for_one)
  end
end
