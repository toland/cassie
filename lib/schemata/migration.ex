defmodule Schemata.Migration do
  @moduledoc ""

  @callback description() :: binary
  @callback authored_at() :: NaiveDateTime.t
  @callback up() :: term
  @callback down() :: term

  @type t :: %__MODULE__{
    description: binary,
    authored_at: integer,
    applied_at:  integer,
    filename:    binary,
    module:      atom
  }

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
      import Schemata

      @behaviour Schemata.Migration

      if unquote(opt_description) do
        def description, do: unquote(opt_description)
      end

      if unquote(opt_authored_at) do
        def authored_at, do: unquote(opt_authored_at)
      end

      def down do
        raise Schemata.MigrationError, [
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
      authored_at: timestamp(module.authored_at)
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

  defp timestamp(date_str) do
    date_str
    |> Timex.parse!("{ISO:Extended:Z}")
    |> DateTime.to_unix(:milliseconds)
  end
end
