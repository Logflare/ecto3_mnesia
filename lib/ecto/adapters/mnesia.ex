defmodule Ecto.Adapters.Mnesia do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Transaction


  alias Ecto.Adapters.Mnesia
  alias Ecto.Adapters.Mnesia.Connection
  alias Ecto.Adapters.Mnesia.Record
  alias Ecto.Adapters.Mnesia.Table

  require Logger

  @impl Ecto.Adapter
  defmacro __before_compile__(_env), do: true

  @impl Ecto.Adapter
  def checkout(_adapter_meta, _config, function) do
    function.()
  end

  @impl Ecto.Adapter
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
        sources: sources,
        query: query,
        sort: sort,
        answers: answers
      }
    },
    params,
    _opts
  ) do
    case :timer.tc(:mnesia, :transaction, [fn ->
      query.(params)
      |> sort.()
      |> answers.()
      |> Enum.map(&Tuple.to_list(&1))
    end]) do
      {time, {:atomic, result}} ->
        Logger.debug("QUERY OK sources=#{inspect(sources)} type=all db=#{time}µs")

        {length(result), result}
      {time, {:aborted, error}} ->
        Logger.debug("QUERY ERROR sources=#{inspect(sources)} type=delete db=#{time}µs #{inspect(error)}")

        {0, []}
    end
  end

  def execute(
    _adapter_meta,
    _query_meta,
    {:nocache,
      %Mnesia.Query{
        type: :update_all,
        sources: sources,
        query: query,
        answers: answers,
        new_record: new_record
      }
    },
    params,
    _opts
  ) do
    {table_name, _schema} = Enum.at(sources, 0)

    case :timer.tc(:mnesia, :transaction, [fn ->
      query.(params)
      |> answers.()
      |> Enum.map(&Tuple.to_list(&1))
      |> Enum.map(fn (record) -> new_record.(record, params) end)
      |> Enum.map(fn (record) ->
        with :ok <- :mnesia.write(table_name, record, :write) do
          Record.to_schema(table_name, record)
        end
      end)
    end]) do
      {time, {:atomic, result}} ->
        Logger.debug("QUERY OK sources=#{inspect(sources)} type=update_all db=#{time}µs")

        {length(result), result}
      {time, {:aborted, error}} ->
        Logger.debug("QUERY ERROR sources=#{inspect(sources)} type=delete db=#{time}µs #{inspect(error)}")

        {0, nil}
    end
  end

  def execute(
    _adapter_meta,
    _query_meta,
    {:nocache,
      %Mnesia.Query{
        type: :delete_all,
        sources: sources,
        query: query,
        answers: answers
      }
    },
    params,
    _opts
  ) do
    {table_name, _schema} = Enum.at(sources, 0)

    case :timer.tc(:mnesia, :transaction, [fn ->
      query.(params)
      |> answers.()
      |> Enum.map(&Tuple.to_list(&1))
      |> Enum.map(fn (record) ->
        :mnesia.delete(table_name, List.first(record), :write)
      end)
    end]) do
      {time, {:atomic, result}} ->
        Logger.debug("QUERY OK sources=#{inspect(sources)} type=delete_all db=#{time}µs")

        {length(result), nil}
      {time, {:aborted, error}} ->
        Logger.debug("QUERY ERROR sources=#{inspect(sources)} type=delete db=#{time}µs #{inspect(error)}")

        {0, nil}
    end
  end

  @impl Ecto.Adapter.Queryable
  def stream(
    _adapter_meta,
    _query_meta,
    {:nocache,
      %Mnesia.Query{query: query, answers: answers}
    },
    params,
    _opts
  ) do
    case :mnesia.transaction(fn ->
      query.(params)
      |> answers.()
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
  def autogenerate(:binary_id), do: Ecto.UUID.generate()

  @impl Ecto.Adapter.Schema
  def insert(
    _adapter_meta,
    %{schema: schema, source: source, autogenerate_id: autogenerate_id},
    params,
    on_conflict,
    returning,
    _opts
  ) do
    table_name = String.to_atom(source)
    context = [
      table_name: table_name,
      schema: schema,
      autogenerate_id: autogenerate_id
    ]
    record = Record.build(params, context)
    id = elem(record, 1)

    case :timer.tc(:mnesia, :transaction, [fn ->
      case on_conflict do
        {:raise, _, _} ->
          with [] <- :mnesia.read(table_name, id, :read),
               :ok <- :mnesia.write(table_name, record, :write) do
            [record]
          else
            [_record] ->
              :mnesia.abort("Record already exists")
          end
        {_, _, _} ->
          with :ok <- :mnesia.write(table_name, record, :write), do: [record]
      end
    end]) do
      {time, {:atomic, [record]}} ->
        result = returning
        |> Enum.map(fn (field) ->
          {field, Record.attribute(record, field, context)}
        end)
        Logger.debug("QUERY OK source=#{inspect(source)} type=insert db=#{time}µs")

        {:ok, result}
      {time, {:aborted, error}} ->
        Logger.debug("QUERY ERROR source=#{inspect(source)} type=delete db=#{time}µs #{inspect(error)}")

        {:invalid, [mnesia: inspect(error)]}
    end
  end

  @impl Ecto.Adapter.Schema
  def insert_all(
    _adapter_meta,
    %{schema: schema, source: source, autogenerate_id: autogenerate_id},
    _header,
    records,
    on_conflict,
    returning,
    _opts
  ) do
    table_name = String.to_atom(source)
    context = [
      table_name: table_name,
      schema: schema,
      autogenerate_id: autogenerate_id
    ]

    case :timer.tc(:mnesia, :transaction, [fn ->
      Enum.map(records, fn (params) ->
        record = Record.build(params, context)
        id = elem(record, 1)
        case on_conflict do
          {:raise, _, _} ->
            with [] <- :mnesia.read(table_name, id, :read),
                 :ok <- :mnesia.write(table_name, record, :write) do
              [record]
            else
              [_record] ->
                :mnesia.abort("Record already exists")
            end
          {_, _, _} ->
            with :ok <- :mnesia.write(table_name, record, :write), do: [record]
        end
      end)
    end]) do
      {time, {:atomic, created_records}} ->
        result = Enum.map(created_records, fn ([record]) ->
          Enum.map(returning, fn (field) ->
            Record.attribute(record, field, context)
          end)
        end)
        Logger.debug("QUERY OK source=#{inspect(source)} type=insert_all db=#{time}µs")

        {length(result), result}
      {time, {:aborted, error}} ->
        Logger.debug("QUERY ERROR source=#{inspect(source)} type=delete db=#{time}µs #{inspect(error)}")

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

    query = Mnesia.Qlc.query(:all, [], [source]).(filters)
    with {selectTime, {:atomic, [attributes]}} <- :timer.tc(:mnesia, :transaction, [fn ->
        query.(params) |> Mnesia.Qlc.answers(nil).()
    end]),
      {updateTime, {:atomic, update}} <- :timer.tc(:mnesia, :transaction, [fn ->
        update = List.zip([Table.attributes(table_name), attributes])
                 |> Record.build(context)
                 |> Record.put_change(params, context)

        with :ok <- :mnesia.write(table_name, update, :write) do
          update
        end
        end]) do
        result = returning
                 |> Enum.map(fn (field) ->
                   {field, Record.attribute(update, field, context)}
                 end)
        Logger.debug("QUERY OK source=#{inspect(source)} type=update db=#{selectTime + updateTime}µs")

        {:ok, result}
    else
      {time, {:atomic, []}} ->
        Logger.debug("QUERY ERROR source=#{inspect(source)} type=delete db=#{time}µs \"No results\"")

        {:error, :stale}
      {time, {:aborted, error}} ->
        Logger.debug("QUERY ERROR source=#{inspect(source)} type=delete db=#{time}µs #{inspect(error)}")

        {:invalid, [mnesia: "#{inspect(error)}"]}
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

    query = Mnesia.Qlc.query(:all, [], [source]).(filters)
    with {selectTime, {:atomic, [[id|_t]]}} <- :timer.tc(:mnesia, :transaction, [fn ->
        query.([]) |> Mnesia.Qlc.answers(nil).()
        |> Enum.map(&Tuple.to_list(&1))
    end]),
      {deleteTime, {:atomic, :ok}} <- :timer.tc(:mnesia, :transaction, [fn ->
        :mnesia.delete(table_name, id, :write)
      end]) do
      Logger.debug("QUERY OK source=#{inspect(source)} type=delete db=#{selectTime + deleteTime}µs")

      {:ok, []}
    else
      {time, {:atomic, []}} ->
        Logger.debug("QUERY ERROR source=#{inspect(source)} type=delete db=#{time}µs \"No results\"")

        {:error, :stale}
      {time, {:aborted, error}} ->
        Logger.debug("QUERY ERROR source=#{inspect(source)} type=delete db=#{time}µs #{inspect(error)}")

        {:invalid, [mnesia: "#{inspect(error)}"]}
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
