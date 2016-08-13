defmodule Schemata.SchemaSpec do
  use ESpec

  alias Schemata.Schema

  @complex_schema "spec/schemas/complex.exs"
  @simple_schema "spec/schemas/simple.exs"

  describe "Schemata.Schema" do
    describe "loading a schema file" do
      before do
        :ok = Schema.load_schema(@complex_schema)
      end

      it "should return the loaded schema file" do
        Schema.schema_file |> should(eq @complex_schema)
      end

      it "should use the saved schema file if none is passed" do
        Schema.load_schema |> should(eq :ok)
        Schema.schema_file |> should(eq @complex_schema)
      end

      it "should return an error if passed a bad file" do
        Schema.load_schema("/does/not/exist") |> should(be_error_result)
      end
    end

    describe "finding tables for a keyspace" do
      before do
        :ok = Schema.load_schema(@complex_schema)
      end

      it "should work for keyspaces with atoms for names" do
        Schema.list_tables(:ks_atom) |> should(have :ks_atom_table)
        Schema.list_tables("ks_atom") |> should(have :ks_atom_table)
      end

      it "should work for keyspaces with binaries for names" do
        Schema.list_tables(:ks_binary) |> should(have :ks_binary_table)
        Schema.list_tables("ks_binary") |> should(have :ks_binary_table)
      end

      it "should work for keyspaces with regular expressions for names" do
        Schema.list_tables(:ks_regex) |> should(have :ks_regex_table)
        Schema.list_tables("ks_regex") |> should(have :ks_regex_table)

        Schema.list_tables(:ks_test_regex) |> should(have :ks_regex_table)
        Schema.list_tables("ks_test_regex") |> should(have :ks_regex_table)

        Schema.list_tables(:ks_bar_foo) |> should(have :ks_regex_table)
        Schema.list_tables("ks_bar_foo") |> should(have :ks_regex_table)
      end

      it "should return '{:error, :unknown_keyspace}' for invalid keyspace" do
        Schema.list_tables(:bad_ks) |> should(eq {:error, :unknown_keyspace})
      end
    end

    describe "ensuring a table" do
      before do
        :ok = Schema.load_schema(@simple_schema)
        :ok = Schemata.drop :view, named: :test_view, in: :schemata_test
        :ok = Schemata.drop :table, named: :test_table, in: :schemata_test
        result = Schema.ensure_table(:schemata_test, :test_table)
        {:shared, result: result}
      end

      # TODO We really need an abstraction for DESCRIBE so that we can
      # introspect on the existing schema.

      describe "when the table doesn't exist" do
        it "should return :ok" do
          shared.result |> should(eq :ok)
        end

        it "should create the table" do
          Schemata.select(:all, from: :test_table, in: :schemata_test)
          |> should(eq [])
        end

        # TODO How do we test this without DESCRIBE?
        it "should create any indexes associated with the table"

        it "should create any views associated with the table" do
          Schemata.select(:all, from: :test_view, in: :schemata_test)
          |> should(eq [])
        end
      end

      describe "when the table already exists" do
        before do
          true = Schemata.insert into: :test_table, in: :schemata_test,
                   values: %{id: "1", data: "A"}

          result = Schema.ensure_table(:schemata_test, :test_table)
          {:shared, result: result}
        end

        it "should return :ok" do
          shared.result |> should(eq :ok)
        end

        it "should not drop the table" do
          Schemata.select(:all, from: :test_view, in: :schemata_test)
          |> should(have_count 1)
        end
      end

      describe "when an invalid keyspace is provided" do
        it "should return '{:error, :unknown_keyspace}'" do
          Schema.ensure_table(:bad_ks, :test_table)
          |> should(eq {:error, :unknown_keyspace})
        end
      end

      describe "when an invalid table is provided" do
        it "should return '{:error, :unknown_table}'" do
          Schema.ensure_table(:schemata_test, :bad_table)
          |> should(eq {:error, :unknown_table})
        end
      end

      describe "when there is an error" do
        before do
          :ok = Schema.load_schema(@complex_schema)
          result = Schema.ensure_table(:ks_atom, :test_table)
          {:shared, result: result}
        end

        it "should return an error result" do
          shared.result |> should(be_error_result)
        end
      end
    end

    describe "creating a table" do
      before do
        :ok = Schema.load_schema(@simple_schema)
        :ok = Schemata.drop :view, named: :test_view, in: :schemata_test
        :ok = Schemata.drop :table, named: :test_table, in: :schemata_test
        result = Schema.create_table(:schemata_test, :test_table)
        {:shared, result: result}
      end

      describe "when the table doesn't exist" do
        it "should return :ok" do
          shared.result |> should(eq :ok)
        end

        it "should create the table" do
          Schemata.select(:all, from: :test_table, in: :schemata_test)
          |> should(eq [])
        end

        # TODO How do we test this without DESCRIBE?
        it "should create any indexes associated with the table"

        it "should create any views associated with the table" do
          Schemata.select(:all, from: :test_view, in: :schemata_test)
          |> should(eq [])
        end
      end

      describe "when the table already exists" do
        before do
          true = Schemata.insert into: :test_table, in: :schemata_test,
                   values: %{id: "1", data: "A"}

          result = Schema.create_table(:schemata_test, :test_table)
          {:shared, result: result}
        end

        it "should return :ok" do
          shared.result |> should(eq :ok)
        end

        it "should drop the table" do
          Schemata.select(:all, from: :test_view, in: :schemata_test)
          |> should(have_count 0)
        end
      end

      describe "when an invalid keyspace is provided" do
        it "should return '{:error, :unknown_keyspace}'" do
          Schema.create_table(:bad_ks, :test_table)
          |> should(eq {:error, :unknown_keyspace})
        end
      end

      describe "when an invalid table is provided" do
        it "should return '{:error, :unknown_table}'" do
          Schema.create_table(:schemata_test, :bad_table)
          |> should(eq {:error, :unknown_table})
        end
      end

      describe "when there is an error" do
        before do
          :ok = Schema.load_schema(@complex_schema)
          result = Schema.create_table(:ks_atom, :test_table)
          {:shared, result: result}
        end

        it "should return an error result" do
          shared.result |> should(be_error_result)
        end
      end
    end

    describe "ensuring a schema" do
      before do
        :ok = Schema.load_schema(@simple_schema)
      end

    end

    describe "applying a schema" do
      before do
        :ok = Schema.load_schema(@simple_schema)
      end

    end
  end
end
