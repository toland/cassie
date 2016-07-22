defmodule SchemataApp do
  use Application

  def start(_type, _args) do
    Schemata.Supervisor.start_link()
  end
end
