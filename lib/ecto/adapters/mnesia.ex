defmodule Ecto.Adapters.Mnesia do
  @moduledoc """
  # Ecto Mnesia Adapter
  This adapter brings the strength of Ecto providing validation, and persistance layer to interact to Mnesia databases.

  Mnesia is Distributed Database Management System shipped with Erlang runtime. Be aware of strengths and weaknesses listed in [erlang documentation](https://erlang.org/doc/man/mnesia.html) before thinking about using it.


  ## What works
  1. Queries
  - [x] Basic all queries
  - [x] Select queries
  - [x] Simple where queries
  - [x] and/or/in in where clauses
  - [x] Bindings
  - [ ] Fragments
  - [x] Limit/Offset queries
  - [x] Sort by one field
  - [ ] Sort by multiple fields
  - [x] One level joins
  - [ ] Deeper joins

  2. Writing operations
  - [x] insert/insert_all
  - [x] update/update_all
  - [x] delete/delete_all
  - [x] Auto incremented ids
  - [x] Binary ids

  Note: supports only on_conflict: :raise/:update_all

  3. Associations
  - [x] has_one associations
  - [x] has_many associations
  - [x] belongs_to associations
  - [ ] many_to_many associations

  4. Transactions
  - [x] Create transactions
  - [x] Rollback transactions

  ## Instalation
  You can include ecto3_mnesia in your dependencies as follow:
  ```
    defp deps do
      ...
      {:ecto3_mnesia, "~> 0.1.0"}, # not released yet
      ...
    end
  ```
  Then configure your application repository to use Mnesia adapter as follow:
  ```
  # ./lib/my_app/repo.ex
  defmodule MyApp.Repo do
    use Ecto.Repo,
      otp_app: :my_app,
      adapter: Ecto.Adapters.Mnesia
  end
  ```

  ## Migrations
  Migrations are not supported yet, you can use mnesia abilities to create tables in a script.
  ```
  # ./priv/repo/mnesia_migration.exs
  IO.inspect :mnesia.create_table(:table_name, [
    disc_copies: [node()],
    record_name: MyApp.Context.Schema,
    attributes: [:id, :field, :updated_at, :inserted_at],
    type: :set
  ])
  ```
  Then run the script with mix `mix run ./priv/repo/mnesia_migration.exs`
  Notice that the table before MUST be defined according to the corresponding schema
  ```
  defmodule MyApp.Context.Schema do
    ...
    schema "table_name" do
      field :field, :string

      timestamps()
    end
    ...
  end
  ```
  """
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Schema
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Transaction


  alias Ecto.Adapters.Mnesia
  alias Ecto.Adapters.Mnesia.Connection
  alias Ecto.Adapters.Mnesia.Record

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
    context = [params: params]
    case :timer.tc(:mnesia, :transaction, [fn ->
      query.(params)
      |> sort.()
      |> answers.(context)
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
    context = [params: params]

    case :timer.tc(:mnesia, :transaction, [fn ->
      query.(params)
      |> answers.(context)
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
    context = [params: params]

    case :timer.tc(:mnesia, :transaction, [fn ->
      query.(params)
      |> answers.(context)
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
    :mnesia.dirty_update_counter({Connection.id_seq_table_name(), :id}, 1)
  end
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
    context = [table_name: table_name, schema: schema, autogenerate_id: autogenerate_id, params: params]

    query = Mnesia.Qlc.query(:all, [], [source]).(filters)
    with {selectTime, {:atomic, [attributes]}} <- :timer.tc(:mnesia, :transaction, [fn ->
        query.(params) |> Mnesia.Qlc.answers(nil, nil).(context)
    end]),
      {updateTime, {:atomic, update}} <- :timer.tc(:mnesia, :transaction, [fn ->
        update = List.zip([schema.__schema__(:fields), attributes])
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
        query.([]) |> Mnesia.Qlc.answers(nil, nil).([params: []])
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
