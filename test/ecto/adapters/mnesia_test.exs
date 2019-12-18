defmodule Ecto.Adapters.MnesiaTest do
  use ExUnit.Case, async: true

  alias EctoMnesia.TestRepo
  alias Ecto.Adapters.Mnesia
  alias Ecto.Adapters.Mnesia.Connection

  defmodule TestSchema do
    use Ecto.Schema

    schema "test_schema" do
      field :field, :string
    end
  end

  setup do
    Mnesia.ensure_all_started([], :permanent)
  end

  test "#start_link" do
    {:ok, repo} = TestRepo.start_link()

    assert Process.alive?(repo)
  end
end
