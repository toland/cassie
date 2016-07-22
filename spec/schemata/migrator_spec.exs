defmodule Schemata.MigratorSpec do
  use ESpec

  alias Schemata.Migrator

  @test_keyspace "schemata_test"
  @base_migrations_path "spec/migrations"

  describe "Schemata.Migrator" do
    before do
      Schemata.truncate table: :migrations, in: @test_keyspace
    end

    finally do
      Migrator.flush
    end

    let :all, do: Migrator.list_migrations
    let :applied, do: Migrator.list_migrations(:applied)
    let :available, do: Migrator.list_migrations(:available)

    describe "in a fresh environment with all migrations available" do
      before do
        migrations_path = Path.join(@base_migrations_path, "all")
        Migrator.load_migrations(migrations_path)
      end

      it "should have no applied migrations" do
        expect applied |> to(be_empty)
      end

      it "should have 3 available migrations" do
        expect available |> to(have_length 3)
      end

      it "should report that all migrations are available" do
        expect all |> to(eq available)
      end
    end

    describe "migrating up a single migration" do
      before do
        migrations_path = Path.join(@base_migrations_path, "one")
        Migrator.load_migrations(migrations_path)
        result = Migrator.migrate(:up)
        {:shared, result: result}
      end

      it "should return '{:ok, :applied}'" do
        expect shared.result |> to(eq {:ok, :applied})
      end

      it "should add an entry into the migrations table" do
        [row] = Schemata.select :all, from: :migrations, in: @test_keyspace
        expect row.applied_at |> to_not(be_nil)
      end

      it "should have 1 applied migration" do
        expect applied |> to(have_length 1)
      end

      it "should have 0 available migrations" do
        expect available |> to(be_empty)
      end

      it "should return '{:ok, :already_applied}' when run again" do
        expect :up |> Migrator.migrate |> to(eq {:ok, :already_applied})
      end

      it "should return an error when attempting to migrate down" do
        expect :down |> Migrator.migrate |> to(be_error_result)
      end
    end

    describe "migrating up two migrations" do
      before do
        migrations_path = Path.join(@base_migrations_path, "two")
        Migrator.load_migrations(migrations_path)
        result = Migrator.migrate(:up)
        {:shared, result: result}
      end

      it "should return '{:ok, :applied}'" do
        expect shared.result |> to(eq {:ok, :applied})
      end

      it "should add two entries into the migrations table" do
        rows = Schemata.select :all, from: :migrations, in: @test_keyspace
        expect rows |> to(have_length 2)
        for row <- rows,
          do: expect row.applied_at |> to_not(be_nil)
      end

      it "should have 2 applied migration" do
        expect applied |> to(have_length 2)
      end

      it "should have 0 available migrations" do
        expect available |> to(be_empty)
      end
    end

    describe "migrating down a single migration" do
      before do
        migrations_path = Path.join(@base_migrations_path, "two")
        Migrator.load_migrations(migrations_path)
        {:ok, :applied} = Migrator.migrate(:up)
        result = Migrator.migrate(:down, 1)
        {:shared, result: result}
      end

      it "should return '{:ok, :applied}'" do
        expect shared.result |> to(eq {:ok, :applied})
      end

      it "should leave a single entry in the migrations table" do
        [row] = Schemata.select :all, from: :migrations, in: @test_keyspace
        expect row.applied_at |> to_not(be_nil)
      end

      it "should have 1 applied migration" do
        expect applied |> to(have_length 1)
      end

      it "should have 1 available migrations" do
        expect available |> to(have_length 1)
      end
    end
  end
end
