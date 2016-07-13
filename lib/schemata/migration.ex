defmodule Schemata.Migration do

  @callback description() :: binary
  @callback authored_at() :: NaiveDateTime.t
  @callback up() :: term
  @callback down() :: term

  defstruct [
    up: nil,
    down: nil,
    description: nil,
    authored_at: nil,
    applied_at: nil,
    filename: nil,
    module: nil
  ]

  defmacro __using__(opts) do
    description = opts[:description]
    authored_at = opts[:authored_at]

    quote do
      @behaviour Schemata.Migration

      if unquote(description) do
        def description, do: unquote(description)
      end

      if unquote(authored_at) do
        def authored_at, do: unquote(authored_at)
      end

      def down do
        raise Schemata.Migrator.MigrationError, message: "Rollback is not supported for migration: #{unquote(description)}"
      end

      defoverridable [down: 0]
    end
  end

  def load(file) do
    {module, _} = Code.load_file(file) |> hd
    %__MODULE__{
      filename: file,
      module: module,
      description: module.description,
      authored_at: module.authored_at
    }
  end
end
