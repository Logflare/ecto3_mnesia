defmodule Ecto.Adapters.Mnesia.Query do
  import Ecto.Adapters.Mnesia.Table, only: [
    record_field_index: 2
  ]

  alias Ecto.Adapters.Mnesia
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.SelectExpr
  require Qlc

  defstruct type: nil, table_name: nil, schema: nil, sources: nil, fields: nil, qlc_query: nil, qlc_sort: nil, new_record: nil

  @type t :: %__MODULE__{
    type: :all | :update_all | :delete_all,
    table_name: atom(),
    schema: atom(),
    sources: Keyword.t(),
    fields: (source :: tuple() -> list(atom())),
    qlc_query: (params :: list() -> qlc_string :: String.t()),
    new_record: (tuple(), list() -> tuple())
  }

  @spec from_ecto_query(type :: atom(), ecto_query :: Ecto.Query.t()) :: mnesia_query :: %Ecto.Adapters.Mnesia.Query{}
  def from_ecto_query(
    type,
    %Ecto.Query{
      select: select,
      joins: joins,
      sources: sources,
      wheres: wheres,
      updates: updates,
      order_bys: order_bys
    } = query
  ) do
    sources = sources(sources)
    {table_name, schema} = Enum.at(sources, 0)

    fields = fields(select, sources)
    qlc_query = Mnesia.Qlc.query(select, joins, sources).(wheres)
    qlc_sort = Mnesia.Qlc.sort(order_bys, select, sources)
    new_record = new_record({table_name, schema}, updates)

    %Mnesia.Query{
      type: type,
      table_name: table_name,
      schema: schema,
      sources: sources,
      fields: fields,
      qlc_query: qlc_query,
      qlc_sort: qlc_sort,
      new_record: new_record
    }
  end

  defp sources(sources) do
    sources
    |> Tuple.to_list()
    |> Enum.map(fn ({table_name, schema, _}) ->
      {String.to_atom(table_name), schema}
    end)
  end

  defp fields(%SelectExpr{fields: fields}, sources) do
    fn (source) ->
      Enum.map(fields, &field(&1, sources))
      |> Enum.reject(&is_nil(&1))
    end
  end
  defp fields(_, _sources) do
    fn ({_, schema}) ->
      schema.__schema__(:fields)
    end
  end

  defp field({{_, _, [{:&, [], [source_index]}, _field]}, [], []}, sources) do
    Enum.at(sources, source_index)
  end

  defp new_record({table_name, schema}, updates) do
    fn (record, params) ->
      case updates do
        [%QueryExpr{expr: [set: replacements]}] ->
          replacements
          |> Enum.reduce(record, fn ({field, {:^, [], [param_index]}}, record) ->
            record_field_index = record_field_index(field, table_name)
            value = Enum.at(params, param_index)
            List.replace_at(record, record_field_index, value)
          end)
          |> List.insert_at(0, schema)
          |> List.to_tuple()
        _ -> record
      end
    end
  end
end
