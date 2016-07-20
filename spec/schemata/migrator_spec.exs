defmodule Schemata.MigratorSpec do
  use ESpec

  alias Schemata.Migrator

  @base_migrations_path "spec/migrations"

  describe "Schemata.Migrator" do
    before do
      Migrator.ensure_migrations_table!
      Schemata.truncate table: :migrations, in: :schemata
    end

    describe "in a fresh environment with all migrations available" do
      let :migrations_path, do: Path.join(@base_migrations_path, "all")

      it "should have no applied migrations" do
        Migrator.migrations_applied |> should(eq [])
      end

      it "should have 3 available migrations" do
        migrations_path
        |> Migrator.migrations_available
        |> length
        |> should(eq 3)
      end

      it "should report all migrations available" do
        available = Migrator.migrations_available(migrations_path)
        migrations = Migrator.migrations(migrations_path)
        migrations |> should(eq available)
      end
    end

    describe "running a single migration" do
      let :migrations_path, do: Path.join(@base_migrations_path, "one")

      before do
        # Schemata.drop :table, named: "users", in: "schemata_test"
        {:ok, :applied} = Migrator.run(migrations_path, :up)
      end

      it "add an entry into the migrations table" do
        [row] = Schemata.select :all, from: "migrations", in: "schemata"
        row.applied_at |> should_not(be_nil)
      end
    end
  end
end
