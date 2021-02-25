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
         {:==, [], [{{:., [], [{:&, [], [source_index]}, :id]}, [], []}, value]},
         %{sources: sources, params: params}
       ) do
    [{source, schema}] = sources
    :mnesia.read(source, value)
  end

  defp to_read(
         {:==, [], [{{:., [], [{:&, [], [source_index]}, field]}, [], []}, value]},
         %{sources: sources, params: params}
       ) do
    [{source, schema}] = sources
    # IO.inspect(field, label: "field")
    # IO.inspect(value, label: "value")
    # IO.inspect(sources, label: "sources")

    :mnesia.all_keys(source) |> IO.inspect(label: "all keys")
    :mnesia.index_read(source, value, field)
    # :mnesia.select(source, [{"$1", [{:==, field, value}], ["$_"]}])
    # |> IO.inspect(label: "select result")
  end

  @spec sort(list(%QueryExpr{}), %SelectExpr{}, list(tuple())) ::
          (query_handle :: :qlc.query_handle() -> query_handle :: :qlc.query_handle())
  def sort([], _select, _sources) do
    fn query -> query end
  end

  def sort(order_bys, select, sources) do
    fn query ->
      Enum.reduce(order_bys, query, fn
        %QueryExpr{expr: expr}, query1 ->
          Enum.reduce(expr, query1, fn {order, field_expr}, query2 ->
            field = field(field_expr, sources)
            field_index = Enum.find_index(fields(select, sources), fn e -> e == field end)

            Qlc.keysort(query2, field_index, order: @order_mapping[order])
          end)
      end)
    end
  end

  defp field({{_, _, [{:&, [], [source_index]}, field]}, [], []}, sources) do
    case Enum.at(sources, source_index) do
      source -> Record.Attributes.to_erl_var(field, source)
    end
  end

  defp fields(%SelectExpr{fields: fields}, sources) do
    Enum.flat_map(sources, fn _source ->
      Enum.map(fields, &field(&1, sources))
      |> Enum.reject(&is_nil(&1))
    end)
  end

  defp fields(:all, [{_table_name, schema} = source | _t]) do
    schema.__schema__(:fields)
    |> Enum.map(&Record.Attributes.to_erl_var(&1, source))
  end

  defp fields(_, [{_table_name, schema} = source | _t]) do
    schema.__schema__(:fields)
    |> Enum.map(&Record.Attributes.to_erl_var(&1, source))
  end
end
