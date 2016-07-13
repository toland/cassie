defmodule Mix.Tasks.Schemata.Gen do
  use Mix.Task
  import Mix.Schemata

  def run([title]) do
    now = System.system_time(:milliseconds)

    template = ~s"""
    -- description: <your-description-here>
    -- authored_at: #{now}
    -- up:
    -- <put your up-migration here>

    -- down:
    -- <put your down-migration here>
    """
    filename = "#{now}_#{title}.cql"
    filepath = Path.join(migrations_path, filename)

    File.write!(filepath, template)

    IO.puts "Created migration template at #{filepath}"
  end
end
