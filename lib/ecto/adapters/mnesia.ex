defmodule Ecto.Adapters.Mnesia do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Transaction

  require Qlc

  alias Ecto.Adapters.Mnesia
  alias Ecto.Adapters.Mnesia.Connection
  alias Ecto.Adapters.Mnesia.Record
  alias Ecto.Adapters.Mnesia.Table

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
  def ensure_all_started(_config, _type) do
    {:ok, _} = Application.ensure_all_started(:mnesia)
    {:ok, []}
  end

  @impl Ecto.Adapter
  def init(config \\ []) do
    {:ok, Connection.child_spec(config), %{}}
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
        qlc_query: qlc_query,
        qlc_sort: qlc_sort,
        qlc_next_answers: qlc_next_answers
      }
    },
    params,
    _opts
  ) do
    case :mnesia.transaction(fn ->
      Qlc.q(qlc_query.(params), [])
      |> qlc_sort.()
      |> qlc_next_answers.()
      |> Enum.map(&Tuple.to_list(&1))
    end) do
      {:atomic, result} ->
        {length(result), result}
      {:aborted, _} -> {0, nil}
    end
  end

  def execute(
    _adapter_meta,
    _query_meta,
    {:nocache,
      %Mnesia.Query{
        type: :update_all,
        table_name: table_name,
        qlc_query: qlc_query,
        new_record: new_record
      }
    },
    params,
    _opts
  ) do
    case :mnesia.transaction(fn ->
      Qlc.q(qlc_query.(params), [])
      |> Qlc.e()
      |> Enum.map(&Tuple.to_list(&1))
      |> Enum.map(fn (record) -> new_record.(record, params) end)
      |> Enum.map(fn (record) ->
        with :ok <- :mnesia.write(table_name, record, :write) do
          Record.to_schema(table_name, record)
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
        qlc_query: qlc_query
      }
    },
    params,
    _opts
  ) do
    case :mnesia.transaction(fn ->
      Qlc.q(qlc_query.(params), []) |> Qlc.e()
      |> Enum.map(&Tuple.to_list(&1))
      |> Enum.map(fn (record) ->
        :mnesia.delete(table_name, List.first(record), :write)
      end)
    end) do
      {:atomic, result} -> {length(result), nil}
      {:aborted, _} -> {0, nil}
    end
  end

  @impl Ecto.Adapter.Queryable
  def stream(
    _adapter_meta,
    _query_meta,
    {:nocache,
      %Mnesia.Query{qlc_query: qlc_query}
    },
    params,
    _opts
  ) do
    case :mnesia.transaction(fn ->
      Qlc.q(qlc_query.(params), []) |> Qlc.e()
      |> Enum.map(&Tuple.to_list(&1))
    end) do
      {:atomic, result} ->
        result
      _ -> []
    end
  end

  @impl Ecto.Adapter.Schema
  def autogenerate(:id) do
    # NOTE /!\ need to call :dets.close/1 on shutdown to close properly table in order to keep state
    :mnesia.dirty_update_counter({Connection.id_seq_table_name(), :id}, 1)
  end
  def autogenerate(:embed_id), do: Ecto.UUID.generate()
  def autogenerate(:binary_id), do: Ecto.UUID.bingenerate()

  @impl Ecto.Adapter.Schema
  def insert(
    _adapter_meta,
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
    record = Record.build(params, context)
    id = elem(record, 1)

    case :mnesia.transaction(fn ->
      with :ok <- :mnesia.write(table_name, record, :write) do
        :mnesia.read(table_name, id)
      end
    end) do
      {:atomic, [record]} ->
        result = returning
        |> Enum.map(fn (field) ->
          {field, Record.attribute(record, field, context)}
        end)
        {:ok, result}
      {:aborted, error} ->
        {:invalid, [mnesia: "#{inspect(error)}"]}
    end
  end

  @impl Ecto.Adapter.Schema
  def insert_all(
    _adapter_meta,
    %{schema: schema, source: source, autogenerate_id: autogenerate_id},
    _header,
    records,
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

    case :mnesia.transaction(fn ->
      Enum.map(records, fn (params) ->
        record = Record.build(params, context)
        id = elem(record, 1)
        with :ok <- :mnesia.write(table_name, record, :write) do
          :mnesia.read(table_name, id)
        end
      end)
    end) do
      {:atomic, created_records} ->
        result = Enum.map(created_records, fn ([record]) ->
          Enum.map(returning, fn (field) ->
            Record.attribute(record, field, context)
          end)
        end)
        {length(result), result}
      {:aborted, _error} ->
        {0, nil}
    end
  end

  @impl Ecto.Adapter.Schema
  def update(
    _adapter_meta,
    %{schema: schema, source: source, autogenerate_id: autogenerate_id},
    params,
    filters,
    returning,
    _opts
  ) do
    table_name = String.to_atom(source)
    source = {table_name, schema}
    context = [table_name: table_name, schema: schema, autogenerate_id: autogenerate_id]

    qlc_query = Mnesia.Qlc.query(:all, [], [source]).(filters)
    with {:atomic, [attributes]} <- :mnesia.transaction(fn ->
        Qlc.q(qlc_query.(params), []) |> Qlc.e()
    end),
      {:atomic, update} <- :mnesia.transaction(fn ->
        update = List.zip([Table.attributes(table_name), attributes])
                 |> Record.build(context)
                 |> Record.put_change(params, context)

        with :ok <- :mnesia.write(table_name, update, :write) do
          update
        end
        end) do
        result = returning
                 |> Enum.map(fn (field) ->
                   {field, Record.attribute(update, field, context)}
                 end)
        {:ok, result}
    else
      {:atomic, []} -> {:error, :stale}
      {:aborted, error} -> {:invalid, [mnesia: "#{inspect(error)}"]}
    end
  end

  @impl Ecto.Adapter.Schema
  def delete(
    _adapter_meta,
    %{schema: schema, source: source},
    filters,
    _opts
  ) do
    table_name = String.to_atom(source)
    source = {table_name, schema}

    qlc_query = Mnesia.Qlc.query(:all, [], [source]).(filters)
    with {:atomic, [[id|_t]]} <- :mnesia.transaction(fn ->
        Qlc.q(qlc_query.([]), []) |> Qlc.e()
        |> Enum.map(&Tuple.to_list(&1))
    end),
      {:atomic, :ok} <- :mnesia.transaction(fn ->
          :mnesia.delete(table_name, id, :write)
        end) do
      {:ok, []}
    else
      {:atomic, []} -> {:error, :stale}
      {:aborted, error} -> {:invalid, [mnesia: "#{inspect(error)}"]}
    end
  end

  @impl Ecto.Adapter.Transaction
  def in_transaction?(_adapter_meta), do: :mnesia.is_transaction()

  @impl Ecto.Adapter.Transaction
  def transaction(_adapter_meta, _options, function) do
    case :mnesia.transaction(fn ->
      function.()
    end) do
      {:atomic, result} -> {:ok, result}
      {:aborted, reason} -> {:error, reason}
    end
  end

  @impl Ecto.Adapter.Transaction
  def rollback(_adapter_meta, value) do
    throw :mnesia.abort(value)
  end

  @impl Ecto.Adapter.Storage
  def storage_up(options) do
    :mnesia.stop()
    case :mnesia.create_schema(options[:nodes] || [node()]) do
      :ok ->
        :mnesia.start()
      {:error, {_, {:already_exists, _}}} ->
        with :ok <- :mnesia.start() do
          {:error, :already_up}
        end
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_down(options) do
    :mnesia.stop()
    case :mnesia.delete_schema(options[:nodes] || [node()]) do
      :ok ->
        :mnesia.start()
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_status(_options) do
    path = List.to_string(:mnesia.system_info(:directory)) <> "/schema.DAT"
    case File.exists?(path) do
      true ->  :up
      false -> :down
    end
  end
end
