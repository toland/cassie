defmodule Schemata.Migration do
  @moduledoc ""

  @callback description() :: binary
  @callback authored_at() :: NaiveDateTime.t
  @callback up() :: term
  @callback down() :: term

  defstruct [
    description: nil,
    authored_at: nil,
    applied_at: nil,
    filename: nil,
    module: nil
  ]

  defmacro __using__(opts) do
    opt_description = opts[:description]
    opt_authored_at = opts[:authored_at]

    quote do
      @behaviour Schemata.Migration

      if unquote(opt_description) do
        def description, do: unquote(opt_description)
      end

      if unquote(opt_authored_at) do
        def authored_at, do: unquote(opt_authored_at)
      end

      def down do
        raise Schemata.Migrator.MigrationError, [
          message: """
          Rollback is not supported for migration: #{unquote(opt_description)}
          """
        ]
      end

      defoverridable [down: 0]
    end
  end

  def load_file(file) do
    {module, _} = file |> Code.load_file |> hd
    %__MODULE__{
      filename: file,
      module: module,
      description: module.description,
      authored_at: module.authored_at
    }
  end

  def from_map(map) do
    map
    |> Enum.into(defaults)
    |> Map.put(:__struct__, __MODULE__)
  end

  defp defaults do
    Map.delete(%__MODULE__{}, :__struct__)
  end
end
