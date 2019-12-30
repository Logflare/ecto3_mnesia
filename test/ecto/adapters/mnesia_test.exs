defmodule Ecto.Adapters.MnesiaAdapterIntegrationTest do
  use ExUnit.Case, async: true

  alias EctoMnesia.TestRepo
  alias Ecto.Adapters.Mnesia

  defmodule TestSchema do
    use Ecto.Schema

    schema "test_schema" do
      field :field, :string
    end
  end

  setup do
    ExUnit.CaptureLog.capture_log fn -> Mnesia.storage_up(nodes: [node()]) end
    Mnesia.ensure_all_started([], :permanent)
    :ok
  end

  describe "Ecto.Adapter#init" do
    test "#start_link" do
      {:ok, repo} = TestRepo.start_link()

      assert Process.alive?(repo)
    end
  end
end
