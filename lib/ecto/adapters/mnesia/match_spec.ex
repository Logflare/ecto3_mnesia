defmodule Ecto.Adapters.Mnesia.MatchSpec do
  import Ecto.Adapters.Mnesia.Table, only: [
    field_index: 2
  ]

  alias Ecto.Query.BooleanExpr

  @type t :: (list(%BooleanExpr{}) | Keyword.t() -> (params :: list() -> :ets.match_spec()))

  @spec build(
    {table_name :: atom(), schema :: struct()}
  ) :: (list(%BooleanExpr{}) | Keyword.t() -> (params :: list() -> :ets.match_spec()))
  def build({table_name, schema}) do
    head = head(table_name, schema)
    result = result()

    fn
      ([%BooleanExpr{}|_t] = wheres) ->
        guards = guards(table_name, wheres)

        fn (params) -> [{head, guards.(params), result}] end
      ([{_k, _v}|_t] = filters) ->
        guards = Enum.map(filters, fn ({key, value}) ->
          index = field_index(key, table_name)
          {:==, :"$#{index}", value}
        end)
        fn (_) -> [{head, guards, result}] end
      ([]) -> fn (_) -> [{head, [], result}] end
    end
  end

  defp head(table_name, schema) do
    attributes = :mnesia.table_info(table_name, :attributes)

    attributes
    |> Enum.with_index()
    |> Enum.map(fn ({_attribute, index}) -> :"$#{index + 1}" end)
    |> Enum.into([])
    |> List.insert_at(0, schema)
    |> List.to_tuple()
  end
  defp guards(table_name, wheres) do
    fn (params) ->
      context = [table_name: table_name, params: params]
      wheres
      |> Enum.map(fn (%BooleanExpr{expr: expr}) -> expr end)
      |> Enum.map(&to_match_spec(&1, context))
    end
  end
  defp result, do: [:"$$"]

  defp to_match_spec(
    {:and, [], [a, b]},
    context
  ), do: {:and, to_match_spec(a, context), to_match_spec(b, context)}
  defp to_match_spec(
    {:or, [], [a, b]},
    context
  ), do: {:or, to_match_spec(a, context), to_match_spec(b, context)}
  defp to_match_spec(
    {:is_nil, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}]},
    _context
  ), do: {:==, field, nil}
  defp to_match_spec(
    {:in, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, {:^, [], [index, length]}]},
    context
  ) do
    values = Enum.slice(context[:params], index, length)
    to_match_spec({:in, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, values]}, context)
  end
  defp to_match_spec(
    {:in, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, values]},
    context
  ) when is_list(values) do
    index = field_index(field, context[:table_name])
    Enum.map(values, fn (value) ->
      {:==, :"$#{index}", value}
    end)
    |> List.insert_at(0, :or)
    |> List.to_tuple()
  end
  defp to_match_spec(
    {op, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, {:^, [], [index]}]},
    context
  ) do
    value = Enum.at(context[:params], index)
    to_match_spec({op, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, value]}, context)
  end
  defp to_match_spec(
    {op, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, value]},
    context
  ) do
    index = field_index(field, context[:table_name])
    {op, :"$#{index}", value}
  end
end
