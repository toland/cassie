defmodule Schemata.SchemaSpec do
  use ESpec

  alias Schemata.Schema

  @schema_file "spec/test_schema.exs"

  describe "Schemata.Schema" do
    before do
      :ok = Schema.load_schema(@schema_file)
    end

    it "should return the loaded schema file" do
      expect Schema.schema_file |> to(eq @schema_file)
    end

    describe "finding tables for a keyspace" do
      it "should work for keyspaces with atoms for names" do
        expect Schema.list_tables(:ks_atom) |> to(have :ks_atom_table)
        expect Schema.list_tables("ks_atom") |> to(have :ks_atom_table)
      end
    end

    it "should work for keyspaces with binaries for names" do
      expect Schema.list_tables(:ks_binary) |> to(have :ks_binary_table)
      expect Schema.list_tables("ks_binary") |> to(have :ks_binary_table)
    end

    it "should work for keyspaces with regular expressions for names" do
      expect Schema.list_tables(:ks_regex) |> to(have :ks_regex_table)
      expect Schema.list_tables("ks_regex") |> to(have :ks_regex_table)

      expect Schema.list_tables(:ks_test_regex) |> to(have :ks_regex_table)
      expect Schema.list_tables("ks_test_regex") |> to(have :ks_regex_table)

      expect Schema.list_tables(:ks_bar_foo) |> to(have :ks_regex_table)
      expect Schema.list_tables("ks_bar_foo") |> to(have :ks_regex_table)
    end
  end
end
