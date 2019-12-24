defmodule Ecto.Adapters.Mnesia do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Queryable

  import Ecto.Adapters.Mnesia.Table, only: [
    field_index: 2,
    attributes: 1
  ]

  alias Ecto.Adapters.Mnesia
  alias Ecto.Adapters.Mnesia.Connection
  alias Ecto.Adapters.Mnesia.Schema

  @id_seq_table_name :id_seq

  @impl Ecto.Adapter
  defmacro __before_compile__(_env), do: true

  @impl Ecto.Adapter
  def checkout(_adapter_meta, _config, function) do
    function.()
  end

  @impl Ecto.Adapter
  def dumpers(:binary_id, type), do: [type, Ecto.UUID]
  def dumpers(_, type), do: [type]


  @impl Ecto.Adapter
  def ensure_all_started(config, _type) do
    :mnesia.create_schema(config[:nodes] || [node()])
    {:ok, _} = Application.ensure_all_started(:mnesia)
    {:ok, []}
  end

  @impl Ecto.Adapter
  def init(config \\ []) do
    ensure_id_seq_table(config[:nodes])
    {:ok, Connection.child_spec(), %{}}
  end

  defp ensure_id_seq_table(nil) do
    ensure_id_seq_table([node()])
  end
  defp ensure_id_seq_table(nodes) when is_list(nodes) do
    case :mnesia.create_table(@id_seq_table_name, [
      ram_copies: nodes,
      attributes: [:id, :_dummy],
      type: :ordered_set
    ]) do
      {:atomic, :ok} ->
        :mnesia.wait_for_tables([@id_seq_table_name], 1_000)
        :ok
      {:aborted, {:already_exists, @id_seq_table_name}} ->
        :already_exists
    end
  end

  @impl Ecto.Adapter
  def loaders(_primitive, type), do: [type]

  @impl Ecto.Adapter.Queryable
  def prepare(type, query) do
    {:nocache, Connection.all(type, query)}
  end

  @impl Ecto.Adapter.Queryable
  def execute(
    _adapter_meta,
    _query_meta,
    {:nocache,
      %Mnesia.Query{
        type: :all,
        table_name: table_name,
        match_spec: match_spec
      }
    },
    params,
    _opts
  ) do
    {:atomic, result} = :mnesia.transaction(fn ->
      # TODO reorder values according to schema ?
      :mnesia.select(table_name, match_spec.(params))
    end)
    {length(result), result}
  end
  def execute(
    _adapter_meta,
    _query_meta,
    {:nocache,
      %Mnesia.Query{
        type: :update_all,
        table_name: table_name,
        match_spec: match_spec,
        new_record: new_record
      }
    },
    params,
    _opts
  ) do
    case :mnesia.transaction(fn ->
      :mnesia.select(table_name, match_spec.(params))
      |> Enum.map(fn (record) -> new_record.(record, params) end)
      |> Enum.map(fn (record) ->
        with :ok <- :mnesia.write(table_name, record, :write) do
          Schema.from_record(table_name, record)
        end
      end)
    end) do
      {:atomic, result} -> {length(result), result}
      {:aborted, _} -> {0, nil}
    end
  end

  def execute(
    _adapter_meta,
    _query_meta,
    {:nocache,
      %Mnesia.Query{
        type: :delete_all,
        table_name: table_name,
        match_spec: match_spec
      }
    },
    params,
    _opts
  ) do
    case :mnesia.transaction(fn ->
      :mnesia.select(table_name, match_spec.(params))
      |> Enum.map(fn (record) ->
        :mnesia.delete(table_name, List.first(record), :write)
      end)
    end) do
      {:atomic, result} -> {length(result), nil}
      {:aborted, e} -> {0, e}
    end
  end

  @impl Ecto.Adapter.Queryable
  def stream(
    _adapter_meta,
    _query_meta,
    {:nocache,
      %Mnesia.Query{table_name: table_name, match_spec: match_spec}
    },
    params,
    _opts
  ) do
    {:atomic, result} = :mnesia.transaction(fn ->
      # TODO reorder values according to schema ?
      :mnesia.select(table_name, match_spec.(params))
    end)
    result
  end

  @impl Ecto.Adapter.Schema
  def autogenerate(:id) do
    id = :mnesia.dirty_update_counter({@id_seq_table_name, :id}, 1)
    # NOTE: dump_tables may be a bottleneck, need to check in production
    spawn(fn -> :mnesia.dump_tables([@id_seq_table_name]) end)
    id
  end
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.bingenerate()

  @impl Ecto.Adapter.Schema
  def insert(
    adapter_meta,
    %{schema: schema, source: source, autogenerate_id: autogenerate_id},
    params,
    _on_conflict,
    returning,
    opts
  ) do
    if opts[:on_conflict] != :replace_all do
      # TODO manage others `on_conflict` configurations
      raise "only [on_conflict: :replace_all] is supported by the adapter"
    end

    table_name = String.to_atom(source)
    context = [
      table_name: table_name,
      schema: schema,
      autogenerate_id: autogenerate_id
    ]
    record = Schema.build_record(params, context)
    id = elem(record, 1)

    case :mnesia.transaction(fn ->
      :mnesia.write(table_name, record, :write)
      :mnesia.read(table_name, id)
    end) do
      {:atomic, [record]} ->
        result = returning
        |> Enum.map(fn (attribute) ->
          {attribute, elem(record, field_index(attribute, table_name))}
        end)
        {:ok, result}
      {:aborted, error} ->
        {:invalid, [mnesia: "#{inspect(error)}"]}
    end
  end
end
