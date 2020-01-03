defmodule Ecto.Adapters.Mnesia.Query do
  import Ecto.Adapters.Mnesia.Table, only: [
    record_field_index: 2
  ]

  alias Ecto.Adapters.Mnesia
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.SelectExpr

  defstruct type: nil, table_name: nil, schema: nil, fields: nil, match_spec: nil, new_record: nil

  @type t :: %__MODULE__{
    type: :all | :update_all | :delete_all,
    table_name: atom(),
    schema: atom(),
    fields: list(atom()),
    match_spec: (params :: list() -> :ets.match_spec()),
    new_record: (tuple(), list() -> tuple())
  }

  @spec from_ecto_query(type :: atom(), ecto_query :: Ecto.Query.t()) :: mnesia_query :: %Ecto.Adapters.Mnesia.Query{}
  def from_ecto_query(
    type,
    %Ecto.Query{select: select, sources: sources, wheres: wheres, updates: updates}
  ) do
    {table_name, schema} = sources(sources)

    fields = fields(select, schema)
    match_spec = Mnesia.MatchSpec.build({table_name, schema}).(wheres)
    new_record = new_record({table_name, schema}, updates)

    %Mnesia.Query{
      type: type,
      table_name: table_name,
      schema: schema,
      fields: fields,
      match_spec: match_spec,
      new_record: new_record
    }
  end

  defp sources(sources) do
    {{table_name, schema, _}} = sources

    {String.to_atom(table_name), schema}
  end

  defp fields(%SelectExpr{fields: fields}, schema) do
    Enum.map(fields, &field(&1))
  end
  defp fields(_, schema), do: schema.__schema__(:fields)

  defp field({{_, _, [{:&, [], [0]}, field]}, [], []}), do: field

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
