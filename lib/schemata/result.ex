defmodule Schemata.Result do
  @moduledoc ""

  use Schemata.CQErl
  alias Schemata.Result

  @opaque t :: record(:cql_result)
  @type value :: CQErl.parameter_val
  @type row   :: map
  @type rows  :: [map]

  @doc "The number of rows in a result set."
  @spec size(Result.t) :: non_neg_integer
  defdelegate size(result), to: CQErl

  @doc "Returns the first row of result, as a map."
  @spec head(Result.t) :: :empty_dataset | row
  def head(result), do: result |> CQErl.head |> maybe_drop_nulls

  @doc "Returns all rows of result, except the first one."
  @spec tail(t) :: Result.t
  defdelegate tail(result), to: CQErl

  @doc "Check to see if there are more results available."
  @spec has_more_pages?(Result.t) :: boolean
  defdelegate has_more_pages?(result), to: CQErl, as: :has_more_pages

  @doc """
  Fetch the next page of result from Cassandra for a given continuation.

  The function will return with the result from Cassandra (synchronously).
  """
  @spec fetch_more(Result.t) :: :no_more_results | {:ok, Result.t}
  defdelegate fetch_more(result), to: CQErl

  @doc """
  Returns a tuple of `{head_row, result_tail}`.

  This can be used to iterate over a result set efficiently. Successively
  call this function over the result set to go through all rows, until it
  returns the `:empty_dataset` atom.
  """
  @spec next(Result.t) :: :empty_dataset | {row, Result.t}
  def next(result) do
    case CQErl.next(result) do
      {row, tail} -> {maybe_drop_nulls(row), tail}
      :empty_dataset -> :empty_dataset
    end
  end

  @doc """
  Extracts rows from a query result.

  Returns a list of rows. Each row is a map of column name, value pairs.
  """
  @spec all_rows(Result.t) :: rows
  def all_rows(result) do
    result |> CQErl.all_rows |> Enum.map(&(maybe_drop_nulls(&1)))
  end

  @doc """
  Extracts the value of the first column of the first row from a query result.
  """
  @spec single_value(Result.t) :: :not_found | value
  def single_value(result) do
    case CQErl.head(result) do
      :empty_dataset -> :not_found
      map ->
        {_, value} = map |> Map.to_list |> hd
        value
    end
  end

  @spec fetch(Result.t, pos_integer) :: :error | {:ok, map}
  def fetch(result, index) do
    case Enum.at(result, index) do
      nil -> :error
      row -> {:ok, row}
    end
  end

  defp maybe_drop_nulls(row) do
    drop? = Application.fetch_env!(:schemata, :drop_nulls)
    maybe_drop_nulls(drop?, row)
  end

  defp maybe_drop_nulls(false, row), do: row
  defp maybe_drop_nulls(true, row) do
    row
    |> Enum.reject(fn {_, v} -> v === :null end)
    |> nillify
    |> Enum.into(%{})
  end

  defp nillify(result) when is_map(result) do
    result
    |> Enum.map(fn {k, v} -> {k, nillify(v)} end)
    |> Enum.into(%{})
  end
  defp nillify(list = [{_key, _value} | _rest]) do
    list
    |> Enum.map(fn {k, v} -> {k, nillify(v)} end)
  end
  defp nillify(list = [_value | _rest]) do
    list
    |> Enum.map(&nillify/1)
  end
  defp nillify(:null), do: nil
  defp nillify(other), do: other

  defimpl Enumerable do
    alias Schemata.Result

    def count(result) do
      {:ok, Result.size(result)}
    end

    def member?(result, row) do
      {:ok, find(Result.next(result), row)}
    end

    def reduce(cursor, acc, reducer)
    def reduce(_result, {:halt, acc}, _fun) do
      {:halted, acc}
    end
    def reduce(result,  {:suspend, acc}, fun) do
      {:suspended, acc, &reduce(result, &1, fun)}
    end
    def reduce(result,  {:cont, acc}, fun) do
      case Result.size(result) do
        0 ->
          maybe_fetch_and_continue(result, acc, fun)

        _n ->
          {h, t} = Result.next(result)
          reduce(t, fun.(h, acc), fun)
      end
    end

    defp maybe_fetch_and_continue(result, acc, fun) do
      if Result.has_more_pages?(result) do
        {:ok, next_page} = result |> Result.fetch_more
        case Result.next(next_page) do
          {h, t} -> reduce(t, fun.(h, acc), fun)
          :empty_dataset -> {:done, acc}
        end
      else
        {:done, acc}
      end
    end

    defp find(:empty_dataset, _row), do: false
    defp find({row2, _tail}, row) when row == row2, do: true
    defp find({_, tail}, row), do: member?(tail, row)
  end
end
