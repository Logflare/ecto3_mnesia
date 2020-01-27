defmodule Ecto.Adapters.Mnesia.Qlc do
  alias Ecto.Adapters.Mnesia.Table
  alias Ecto.Adapters.Mnesia.Record
  alias Ecto.Query.BooleanExpr
  alias Ecto.Query.SelectExpr

  def build(select, joins, sources) do
    fn
      ([%BooleanExpr{}] = wheres) ->
        select = select(select, sources)
        fn (params) ->
          context = %{sources: sources, params: params}
          qualifiers = qualifiers(wheres, context)

          joins = joins(joins, context)
          comprehension = [select, Enum.join(joins, ", "), Enum.join(qualifiers, ", ")]
          |> Enum.reject(fn (component) -> String.length(component) == 0 end)
          |> Enum.join(", ")
          "[#{comprehension}]"
        end
      (filters) ->
        fn (params) ->
          context = %{sources: sources, params: params}
          select = select(select, sources)
          qualifiers = qualifiers(filters, context)

          joins = joins(joins, context)
          comprehension = [select, Enum.join(joins, ", "), Enum.join(qualifiers, ", ")]
          |> Enum.reject(fn (component) -> String.length(component) == 0 end)
          |> Enum.join(", ")
          "[#{comprehension}]"
        end
    end
  end

  defp select(select, sources) do
    fields = fields(select, sources)

    "[#{Enum.join(fields, ", ")}] || " <>
      (Enum.map(sources, fn ({table_name, _schema} = source) ->
        "#{record_pattern(source)} <- mnesia:table('#{table_name}')"
      end) |> Enum.join(", "))
  end

  defp fields(%SelectExpr{fields: fields}, sources) do
    Enum.flat_map(sources, fn (_source) ->
      Enum.map(fields, &field(&1, sources))
      |> Enum.reject(&is_nil(&1))
    end)
  end
  defp fields(:all, [{_table_name, schema} = source|_t]) do
    schema.__schema__(:fields)
    |> Enum.map(&Record.Attributes.to_erl_var(&1, source))
  end
  defp fields(_, [{_table_name, schema} = source|_t]) do
    schema.__schema__(:fields)
    |> Enum.map(&Record.Attributes.to_erl_var(&1, source))
  end

  defp field({{_, _, [{:&, [], [source_index]}, field]}, [], []}, sources) do
    case Enum.at(sources, source_index) do
      source -> Record.Attributes.to_erl_var(field, source)
    end
  end

  defp qualifiers(wheres, context) do
    wheres
    |> Enum.map(fn
      (%BooleanExpr{expr: expr}) -> expr
      ({field, value}) -> {field, value}
    end)
    |> Enum.map(&to_qlc(&1, context))
  end

  defp joins(joins, context) do
    joins
    |> Enum.map(fn (%{on: %{expr: expr}}) -> expr end)
    |> Enum.map(&to_qlc(&1, context))
  end

  defp record_pattern(source) do
    record_pattern_attributes = record_pattern_attributes(source)
    "{#{Enum.join(record_pattern_attributes, ", ")}}"
  end

  defp record_pattern_attributes({table_name, _schema} = source) do
    Table.attributes(table_name)
    |> Enum.map(fn (attribute) -> Record.Attributes.to_erl_var(attribute, source) end)
    |> List.insert_at(0, "Schema")
  end

  defp to_qlc({field, value}, %{sources: [source]}) do
    erl_var = Record.Attributes.to_erl_var(field, source)
    "#{erl_var} == #{to_erl(value)}"
  end
  defp to_qlc(
    {:and, [], [a, b]},
    context
  ), do: "(#{to_qlc(a, context)} andalso #{to_qlc(b, context)})"
  defp to_qlc(
    {:or, [], [a, b]},
    context
  ), do: "(#{to_qlc(a, context)} orelse #{to_qlc(b, context)})"
  defp to_qlc(
    {:is_nil, [], [{{:., [], [{:&, [], [source_index]}, field]}, [], []}]},
    context
  ) do
    source = Enum.at(context[:sources], source_index)
    erl_var = Record.Attributes.to_erl_var(field, source)
    "#{erl_var} == nil"
  end
    defp to_qlc(
      {:in, [], [{{:., [], [{:&, [], [source_index]}, field]}, [], []}, {:^, [], [index, length]}]},
      context
    ) do
    values = Enum.slice(context[:params], index, length)
    to_qlc({:in, [], [{{:., [], [{:&, [], [source_index]}, field]}, [], []}, values]}, context)
    end
  defp to_qlc(
    {:in, [], [{{:., [], [{:&, [], [source_index]}, field]}, [], []}, values]},
    context
  ) when is_list(values) do
    source = Enum.at(context[:sources], source_index)
    erl_var = Record.Attributes.to_erl_var(field, source)
    "lists:member(#{erl_var}, [#{to_erl(values) |> Enum.join(", ")}])"
  end
  defp to_qlc(
    {op, [], [{{:., [], [{:&, [], [source_index]}, field]}, [], []}, {:^, [], [index]}]},
    context
  ) do
    value = Enum.at(context[:params], index)
    to_qlc({op, [], [{{:., [], [{:&, [], [source_index]}, field]}, [], []}, value]}, context)
  end
  defp to_qlc(
    {op, [], [{{:., [], [{:&, [], [key_source_index]}, key_field]}, [], []}, {{:., [], [{:&, [], [value_source_index]}, value_field]}, [], []}]},
    context
  ) do
    key_source = Enum.at(context[:sources], key_source_index)
    value_source = Enum.at(context[:sources], value_source_index)
    erl_var = Record.Attributes.to_erl_var(key_field, key_source)
    value = Record.Attributes.to_erl_var(value_field, value_source)
    "#{erl_var} #{op} #{value}"
  end
  defp to_qlc(
    {op, [], [{{:., [], [{:&, [], [source_index]}, field]}, [], []}, value]},
    context
  ) do
    source = Enum.at(context[:sources], source_index)
    erl_var = Record.Attributes.to_erl_var(field, source)
    value = to_erl(value)
    "#{erl_var} #{op} #{value}"
  end

  defp to_erl(values) when is_list(values), do: Enum.map(values, &to_erl(&1))
  defp to_erl(value) when is_binary(value), do: ~s(<<"#{value}">>)
  defp to_erl(value), do: value
end
