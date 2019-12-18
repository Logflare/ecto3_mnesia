defmodule Ecto.Adapters.Mnesia.Connection do
  use Supervisor

  alias Ecto.Adapters.Mnesia.Connection

  def start_link() do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl Supervisor
  def init(_init_arg) do
    children = []

    Supervisor.init(children, strategy: :one_for_one)
  end

  def child_spec do
    %{
      id: Connection,
      start: {Connection, :start_link, []},
      type: :supervisor
    }
  end
end
