defmodule Ecto.Adapters.MnesiaAdapterIntegrationTest do
  use ExUnit.Case, async: true

  alias EctoMnesia.TestRepo
  alias Ecto.Adapters.Mnesia

  setup do
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