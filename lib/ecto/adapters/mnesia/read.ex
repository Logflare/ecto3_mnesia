defmodule Ecto.Adapters.Mnesia.Read do
  @moduledoc false
  require Qlc

  alias Ecto.Adapters.Mnesia.Record
  alias Ecto.Adapters.Mnesia.Table
  alias Ecto.Query.BooleanExpr
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.SelectExpr

  @order_mapping %{
    asc: :ascending,
    desc: :descending
  }

  def query(select, joins, sources, wheres) do
    fn params ->
      context = %{sources: sources, params: params}

      [where] = wheres

      to_read(where.expr, context)
    end
  end

  defp to_read(
         {:==, [], [{{:., [], [{:&, [], [source_index]}, field]}, [], []}, {:^, [], [index]}]},
         context
       ) do
    value = Enum.at(context[:params], index)
    to_read({:==, [], [{{:., [], [{:&, [], [source_index]}, field]}, [], []}, value]}, context)
  end

  defp to_read(
         {:==, [], [{{:., [], [{:&, [], [source_index]}, field]}, [], []}, value]},
         %{sources: sources, params: params}
       ) do
    [{source, schema}] = sources
    :mnesia.read(source, value)
  end
end
