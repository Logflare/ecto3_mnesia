defmodule Ecto.Adapters.Mnesia.Query do
  import Ecto.Adapters.Mnesia.Table, only: [
    field_index: 2
  ]

  alias Ecto.Adapters.Mnesia
  alias Ecto.Query.QueryExpr

  defstruct type: nil, table_name: nil, match_spec: nil, new_record: nil

  @type t :: %__MODULE__{
    type: :all | :update_all | :delete_all,
    table_name: atom(),
    match_spec: (params :: list() -> :ets.match_spec()),
    new_record: (tuple(), list() -> tuple())
  }

  @spec from_ecto_query(type :: :all | :update_all | :delete_all, ecto_query :: Ecto.Query.t()) :: mnesia_query :: t()
  def from_ecto_query(
    type,
    %Ecto.Query{sources: sources, wheres: wheres, updates: updates}
  ) do
    {table_name, schema} = sources(sources)

    match_spec = Mnesia.MatchSpec.build({table_name, schema}, wheres)
    new_record = new_record({table_name, schema}, updates)

    %Mnesia.Query{
      type: type,
      table_name: table_name,
      match_spec: match_spec,
      new_record: new_record
    }
  end

  defp sources(sources) do
    {{table_name, schema, _}} = sources

    {String.to_atom(table_name), schema}
  end

  defp new_record({table_name, schema}, updates) do
    fn (record, params) ->
      case updates do
        [%QueryExpr{expr: [set: replacements]}] ->
          replacements
          |> Enum.reduce(record, fn ({field, {:^, [], [param_index]}}, record) ->
            field_index = field_index(field, table_name)
            value = Enum.at(params, param_index)

            List.replace_at(record, field_index, value)
          end)
          |> List.insert_at(0, schema)
          |> List.to_tuple()
        _ -> record
      end
    end
  end
end
