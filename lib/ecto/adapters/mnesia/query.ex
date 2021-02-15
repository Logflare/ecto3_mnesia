defmodule Ecto.Adapters.Mnesia.Query do
  @moduledoc false
  import Ecto.Adapters.Mnesia.Table,
    only: [
      record_field_index: 2
    ]

  alias Ecto.Adapters.Mnesia
  alias Ecto.Query.QueryExpr
  require Qlc

  defstruct type: nil,
            codepath: nil,
            sources: nil,
            query: nil,
            sort: nil,
            answers: nil,
            new_record: nil

  @type t :: %__MODULE__{
          codepath: :qlc | :read,
          type: :all | :update_all | :delete_all,
          sources: Keyword.t(),
          query: (params :: list() -> query_handle :: :qlc.query_handle()),
          sort: (query_handle :: :qlc.query_handle() -> query_handle :: :qlc.query_handle()),
          answers: (query_handle :: :qlc.query_handle(), context :: Keyword.t() -> list(tuple())),
          new_record: (tuple(), list() -> tuple())
        }

  alias Ecto.Query.BooleanExpr
  alias Ecto.Query.QueryExpr
  alias Ecto.Query.SelectExpr

  @spec from_ecto_query(type :: atom(), ecto_query :: Ecto.Query.t()) :: mnesia_query :: t()
  def from_ecto_query(type, ecto_query) do
    cond do
      is_simple_ecto_where_expr?(ecto_query) -> do_from_ecto_query(type, ecto_query, :read)
      true -> do_from_ecto_query(type, ecto_query)
    end
  end

  defp is_simple_ecto_where_expr?(%Ecto.Query{wheres: [where], order_bys: nil}) do
    %Ecto.Query.BooleanExpr{
      expr: expr,
      op: :and,
      params: nil,
      subqueries: []
    } = where

    match_simple_where_expr?(expr, :variant_1) || match_simple_where_expr?(expr, :variant_2)
  end

  defp is_simple_ecto_where_expr?(_), do: false

  defp match_simple_where_expr?(expr, :variant_1) do
    match?(
      {:==, [], [{{:., [], [{:&, [], [0], field}]}, [], []}, {:^, [], [0]}]} when is_atom(field),
      expr
    )
  end

  defp match_simple_where_expr?(expr, :variant_2) do
    match?(
      {:==, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, {:^, [], [0]}]}
      when is_atom(field),
      expr
    )
  end

  @spec from_ecto_query(type :: atom(), ecto_query :: Ecto.Query.t()) :: mnesia_query :: t()
  defp do_from_ecto_query(
         type,
         %Ecto.Query{
           select: select,
           joins: [] = joins,
           sources: sources,
           wheres: wheres,
           updates: [] = updates,
           order_bys: [] = order_bys,
           limit: nil = limit,
           offset: nil = offset
         } = eq,
         :read
       ) do
    {{table, schema, nil}} = sources
    context = %{sources: sources}
    sources = sources(sources)

    query = Mnesia.Read.query(select, joins, sources, wheres)

    %Mnesia.Query{
      type: type,
      query: query,
      sources: sources,
      codepath: :read
    }
  end

  @spec from_ecto_query(type :: atom(), ecto_query :: Ecto.Query.t()) ::
          mnesia_query :: t()
  defp do_from_ecto_query(
         type,
         %Ecto.Query{
           select: select,
           joins: joins,
           sources: sources,
           wheres: wheres,
           updates: updates,
           order_bys: order_bys,
           limit: limit,
           offset: offset
         }
       ) do
    sources = sources(sources)
    query = Mnesia.Qlc.query(select, joins, sources).(wheres)
    sort = Mnesia.Qlc.sort(order_bys, select, sources)
    answers = Mnesia.Qlc.answers(limit, offset)
    new_record = new_record(Enum.at(sources, 0), updates)

    %Mnesia.Query{
      type: type,
      sources: sources,
      query: query,
      sort: sort,
      answers: answers,
      new_record: new_record,
      codepath: :qlc
    }
  end

  defp sources(sources) do
    sources
    |> Tuple.to_list()
    |> Enum.map(fn {table_name, schema, _} ->
      {String.to_atom(table_name), schema}
    end)
  end

  defp new_record({table_name, schema}, updates) do
    fn record, params ->
      case updates do
        [%QueryExpr{expr: [set: replacements]}] ->
          replacements
          |> Enum.reduce(record, fn {field, {:^, [], [param_index]}}, record ->
            record_field_index = record_field_index(field, table_name)
            value = Enum.at(params, param_index)
            List.replace_at(record, record_field_index, value)
          end)
          |> List.insert_at(0, schema)
          |> List.to_tuple()

        _ ->
          record
      end
    end
  end
end
