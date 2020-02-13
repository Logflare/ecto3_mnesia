defmodule Ecto.Adapters.Mnesia.Connection do
  use GenServer

  alias Ecto.Adapters.Mnesia
  alias Ecto.Adapters.Mnesia.Connection

  @id_seq_table_name :id_seq

  def start_link(config) do
    GenServer.start_link(Connection, [config], name: __MODULE__)
  end

  @impl GenServer
  def init(config) do
    Process.flag(:trap_exit, true)
    :mnesia.stop()
    :mnesia.create_schema([node()])
    :mnesia.start()
    ensure_id_seq_table(config[:nodes])

    {:ok, config}
  end

  @impl GenServer
  def terminate(_reason, state) do
    spawn fn ->
      try do
        :dets.sync(@id_seq_table_name)
        state
      rescue
        e -> e
      end
    end
  end

  def id_seq_table_name, do: @id_seq_table_name

  def all(type, %Ecto.Query{} = query) do
    Mnesia.Query.from_ecto_query(type, query)
  end

  defp ensure_id_seq_table(nil) do
    ensure_id_seq_table([node()])
  end
  defp ensure_id_seq_table(nodes) when is_list(nodes) do
    case :mnesia.create_table(@id_seq_table_name, [
      disc_only_copies: nodes,
      attributes: [:id, :_dummy],
      type: :set
    ]) do
      {:atomic, :ok} ->
        :mnesia.wait_for_tables([@id_seq_table_name], 1_000)
      {:aborted, {:already_exists, @id_seq_table_name}} ->
        :already_exists
    end
  end
end
