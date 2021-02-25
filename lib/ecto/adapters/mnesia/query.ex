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
      is_simple_ecto_where_expr?(ecto_query) and match_simple_where_expr?(ecto_query, :id) ->
        do_from_ecto_query(type, ecto_query, :read)

      is_simple_ecto_where_expr?(ecto_query) and
        match_simple_where_expr?(ecto_query, :non_id_field) and index_exists?(ecto_query) ->
        do_from_ecto_query(type, ecto_query, :index_read)

      true ->
        do_from_ecto_query(type, ecto_query)
    end
  end

  defp is_simple_ecto_where_expr?(%Ecto.Query{
         select: %SelectExpr{expr: {:&, [], [0]}},
         wheres: [where],
         order_bys: []
       }) do
    match?(
      %Ecto.Query.BooleanExpr{
        expr: expr,
        op: :and,
        params: nil,
        subqueries: []
      },
      where
    )
  end

  defp is_simple_ecto_where_expr?(_), do: false

  defp match_simple_where_expr?(%{wheres: [%{expr: expr}]}, :id) do
    match?(
      {:==, [], [{{:., [], [{:&, [], [0], field}]}, [], []}, {:^, [], [0]}]} when field == :id,
      expr
    ) or
      match?(
        {:==, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, {:^, [], [0]}]} when field == :id,
        expr
      )
  end

  defp match_simple_where_expr?(%{wheres: [%{expr: expr}]}, :non_id_field) do
    match?(
      {:==, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, {:^, [], [0]}]}
      when field != :id,
      expr
    )
  end

  defp index_exists?(%Ecto.Query{
         wheres: [where],
         select: select,
         sources: sources
       }) do
    fields_in_correct_order = for {{_, _, [_, field]}, _, _} <- select.fields, do: field
    field = get_field(where.expr)
    [{tab, _schema}] = sources(sources)

    index_exists?(tab, field)
  end

  defp get_field({:==, [], [{{:., [], [{:&, [], [0]}, field]}, [], []}, {:^, [], [0]}]}) do
    field
  end

  defp index_exists?(table, field) when is_atom(field) and is_atom(table) do
    attrs = :mnesia.table_info(table, :attributes)
    field_pos = Enum.find_index(attrs, &(&1 == field))

    index = :mnesia.table_info(table, :index)
    (field_pos + 2) in index
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
         codepath
       )
       when codepath in [:read, :index_read] do
    sources = sources(sources)
    [{table, _schema}] = sources

    queryfn = Mnesia.Read.query(select, joins, sources, wheres)

    sort = fn queryfn_result ->
      fields_in_correct_order = for {{_, _, [_, field]}, _, _} <- select.fields, do: field
      attributes = :mnesia.table_info(table, :attributes)

      queryfn_result
      |> Enum.map(&Enum.zip(attributes, &1))
      |> Enum.map(fn kv_list ->
        Enum.sort_by(kv_list, fn {k, _} ->
          Enum.find_index(fields_in_correct_order, &(&1 == k))
        end)
      end)
      |> Enum.map(fn kv_list -> Enum.map(kv_list, fn {_, v} -> v end) end)
    end

    %Mnesia.Query{
      type: type,
      query: queryfn,
      sort: sort,
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
