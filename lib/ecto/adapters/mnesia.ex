defmodule Ecto.Adapters.Mnesia do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Queryable

  alias Ecto.Adapters.Mnesia
  alias Ecto.Adapters.Mnesia.Connection
  alias Ecto.Adapters.Mnesia.Schema

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
  def init(_config) do
    {:ok, Connection.child_spec(), %{}}
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
          Schema.from_mnesia(table_name, record)
        end
      end)
    end) do
      {:atomic, result} -> {length(result), result}
      {:aborted, _} -> {0, nil}
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
end
