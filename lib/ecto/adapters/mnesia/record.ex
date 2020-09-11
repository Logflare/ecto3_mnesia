defmodule Ecto.Adapters.Mnesia.Record do
  @moduledoc false
  import Ecto.Adapters.Mnesia.Table,
    only: [
      attributes: 1,
      field_index: 2,
      field_name: 2
    ]

  alias Ecto.Adapters.Mnesia

  @type t :: tuple()

  @spec to_schema(table_name :: atom(), record :: t()) :: struct()
  def to_schema(table_name, record) do
    Enum.reduce(
      attributes(table_name),
      # schema struct
      struct(elem(record, 0)),
      fn attribute, struct ->
        %{struct | attribute => elem(record, field_index(attribute, table_name))}
      end
    )
  end

  @spec build(params :: Keyword.t(), context :: Keyword.t()) :: record :: t()
  def build(params, context) do
    table_name = context[:table_name]
    schema = context[:schema]

    {key, _source, type} = context[:autogenerate_id] || {nil, nil, nil}

    attributes(table_name)
    |> Enum.map(fn
      ^key ->
        params[key] ||
          Mnesia.autogenerate(type)

      :inserted_at ->
        # TODO Repo#insert_all do not set timestamps, pickup Repo timestamps configuration
        params[:inserted_at] ||
          NaiveDateTime.utc_now()

      :updated_at ->
        # TODO Repo#insert_all do not set timestamps, pickup Repo timestamps configuration
        params[:updated_at] ||
          NaiveDateTime.utc_now()

      attribute ->
        case Keyword.fetch(params, attribute) do
          {:ok, value} -> value
          :error -> nil
        end
    end)
    |> List.insert_at(0, schema)
    |> List.to_tuple()
  end

  @spec put_change(record :: t(), params :: Keyword.t(), context :: Keyword.t()) :: record :: t()
  def put_change(record, params, context) do
    table_name = context[:table_name]
    schema = context[:schema]

    record
    |> Tuple.to_list()
    |> List.delete_at(0)
    |> Enum.with_index()
    |> Enum.map(fn {attribute, index} ->
      case Keyword.fetch(params, field_name(index, table_name)) do
        {:ok, value} -> value
        :error -> attribute
      end
    end)
    |> List.insert_at(0, schema)
    |> List.to_tuple()
  end

  @spec attribute(record :: t(), field :: atom(), context :: Keyword.t()) :: atribute :: any()
  def attribute(record, field, context) do
    table_name = context[:table_name]

    elem(record, field_index(field, table_name))
  end

  defmodule Attributes do
    @moduledoc false

    @type t :: list()

    @spec to_erl_var(attribute :: atom(), source :: tuple()) :: erl_var :: String.t()
    def to_erl_var(attribute, {_table_name, schema}) do
      (schema |> to_string() |> String.split(".") |> List.last()) <>
        (attribute |> Atom.to_string() |> String.capitalize())
    end
  end
end
