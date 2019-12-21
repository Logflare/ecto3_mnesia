defmodule Ecto.Adapters.Mnesia.Query do
  alias Ecto.Adapters.Mnesia
  alias Ecto.Query.BooleanExpr

  defstruct table_name: nil, match_spec: nil

  @type t :: %__MODULE__{
    table_name: atom(),
    match_spec: (params :: list() -> :ets.match_spec())
  }

  @spec from_ecto_query(ecto_query :: Ecto.Query.t()) :: mnesia_query :: t()
  def from_ecto_query(%Ecto.Query{sources: sources, wheres: wheres}) do
    {{table_name, schema, _}} = sources
    table_name = String.to_atom(table_name)
    attributes = :mnesia.table_info(table_name, :attributes)

    head = [schema] ++ (attributes
           |> Enum.with_index()
           |> Enum.map(fn ({_attribute, index}) -> :"$#{index + 1}" end)
           |> Enum.into([]))
           |> List.to_tuple()
    guards = fn (params) ->
      context = [table_name: table_name, params: params]
      wheres
      |> Enum.map(fn (%BooleanExpr{expr: expr}) -> expr end)
      |> Enum.map(&to_match_spec(&1, context))
    end
    result = [:"$$"]

    %Mnesia.Query{
      table_name: table_name,
      match_spec: fn (params) ->
        [{head, guards.(params), result}]
      end
    }
  end

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
    ([:or] ++ Enum.map(values, fn (value) ->
      {:==, :"$#{index + 1}", value}
    end))
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
    {op, :"$#{index + 1}", value}
  end

  defp field_index(field, table_name) do
    attributes = :mnesia.table_info(table_name, :attributes)
    index = Enum.find_index(attributes, fn (attribute) -> attribute == field end)
  end
end
