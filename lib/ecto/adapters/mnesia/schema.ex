defmodule Ecto.Adapters.Mnesia.Schema do
  import Ecto.Adapters.Mnesia.Table, only: [
    attributes: 1,
    field_index: 2
  ]

  alias Ecto.Adapters.Mnesia

  @spec from_record(table_name :: atom(), record :: tuple) :: struct()
  def from_record(table_name, record) do
    Enum.reduce(
      attributes(table_name),
      struct(elem(record, 0)), # schema struct
      fn (attribute, struct) ->
        %{struct |
          attribute => elem(record, field_index(attribute, table_name))
        }
      end
    )
  end

  @spec build_record(params :: Keyword.t(), context :: Keyword.t()) :: record :: tuple()
  def build_record(params, context) do
    table_name = context[:table_name]
    schema = context[:schema]
    {key, _source, type} = context[:autogenerate_id]

    attributes(table_name)
    |> Enum.map(fn
      (^key) ->
        params[key] ||
          Mnesia.autogenerate(type)
      (:inserted_at) ->
        params[:inserted_at] ||
          # TODO Repo#insert_all do not set timestamps, pickup Repo timestamps configuration
          NaiveDateTime.utc_now()
      (:updated_at) ->
        params[:updated_at] ||
          # TODO Repo#insert_all do not set timestamps, pickup Repo timestamps configuration
          NaiveDateTime.utc_now()
      (attribute) ->
        case Keyword.fetch(params, attribute) do
          {:ok, value} -> value
          :error -> nil
        end
    end)
    |> List.insert_at(0, schema)
    |> List.to_tuple()
  end
end
