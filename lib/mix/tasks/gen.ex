defmodule Mix.Tasks.Schemata.Gen do
  @shortdoc "Generate a new migration"
  @moduledoc """
  Generate a new migration.

  Provide a name in lower case with underscores:

      mix schemata.gen my_new_migration
  """

  use Timex
  use Mix.Task
  import Mix.Generator
  import Mix.Schemata

  def run([title]) do
    create_directory migrations_path

    now = Timex.now

    template = """
    defmodule Schemata.#{Inflex.camelize(title)}Migration do
      use Schemata.Migration, [
        authored_at: "#{Timex.format!(now, "%FT%TZ", :strftime)}",
        description: "<your-description-here>"
      ]

      def up do
        # <put your up-migration here>
        :ok
      end

      def down do
        # <put your down-migration here>
        raise Schemata.MigrationError, message: "Cannot rollback"
      end
    end
    """
    filename = "#{Timex.format!(now, "%Y%m%d%H%M%S", :strftime)}_#{title}.exs"
    filepath = Path.join(migrations_path, filename)

    create_file(filepath, template)
  end
  def run([]) do
    IO.puts "Error: migration name required"
  end
  def run(_) do
    IO.puts "Error: too many arguments"
  end
end
