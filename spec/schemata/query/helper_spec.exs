defmodule Schemata.Query.HelperSpec do
  use ESpec

  alias Schemata.Query.Helper

  describe "query_from_opts/2" do
    xit "should raise an exception if a field is missing" do
      expect Helper.query_from_opts([], required: [:foo])
      |> to(raise_exception ArgumentError)
    end
  end
end
