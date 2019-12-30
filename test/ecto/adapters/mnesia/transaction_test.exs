defmodule Ecto.Adapters.MnesiaTransactionIntegrationTest do
  use ExUnit.Case, async: false

  alias EctoMnesia.TestRepo
  alias Ecto.Adapters.Mnesia

  @table_name __MODULE__.Table

  defmodule TestSchema do
    use Ecto.Schema

    schema "#{Ecto.Adapters.MnesiaTransactionIntegrationTest.Table}" do
      field :field, :string
    end
  end

  setup_all do
    ExUnit.CaptureLog.capture_log fn -> Mnesia.storage_up(nodes: [node()]) end
    Mnesia.ensure_all_started([], :permanent)
    {:ok, _repo} = TestRepo.start_link()

    :mnesia.create_table(@table_name, [
      ram_copies: [node()],
      record_name: TestSchema,
      attributes: [:id, :field],
      storage_properties: [
        ets: [:compressed]
      ],
      type: :ordered_set
    ])
    :mnesia.wait_for_tables([@table_name], 1000)
  end

  describe "Ecto.Adapter.Transaction" do
    test "#transaction should execute" do
      assert TestRepo.transaction(fn ->
        TestRepo.all(TestSchema)
      end) == {:ok, []}
    end

    test "#rollback should rollback" do
      assert TestRepo.transaction(fn ->
        TestRepo.rollback(:reason)
      end) == {:error, :reason}
    end

    test "#in_transaction should return false" do
      assert TestRepo.in_transaction?() == false
    end

    test "#in_transaction should return true in transaction" do
      TestRepo.transaction fn ->
        assert TestRepo.in_transaction?() == false
      end
    end
  end
end
