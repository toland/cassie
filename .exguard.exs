use ExGuard.Config

guard("compile", run_on_start: true)
|> command("mix")
|> watch(~r{\.(erl|ex|exs|eex|xrl|yrl)\z}i)
|> notification(:off)

guard("specs", run_on_start: true)
|> command("mix spec --cover")
|> watch({~r{lib/(?<dir>.+)/(?<file>.+).ex$}, fn(m) -> "spec/#{m["dir"]}/#{m["file"]}_spec.exs" end})
|> watch(~r{\.(erl|ex|exs|eex|xrl|yrl)\z}i)
