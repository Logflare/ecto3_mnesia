defmodule Ecto.Adapters.Mnesia.SchemaIntegrationTest do
  use ExUnit.Case, async: false

  alias EctoMnesia.TestRepo
  alias Ecto.Adapters.Mnesia

  @table_name __MODULE__.Table

  defmodule TestSchema do
    use Ecto.Schema

    schema "#{Ecto.Adapters.Mnesia.SchemaIntegrationTest.Table}" do
      field :field, :string

      timestamps()
    end
  end

  setup_all do
    Mnesia.ensure_all_started([], :permanent)
    {:ok, _repo} = TestRepo.start_link()

    :mnesia.create_table(@table_name, [
      ram_copies: [node()],
      record_name: TestSchema,
      attributes: [:id, :field, :inserted_at, :updated_at],
      storage_properties: [ ets: [:compressed] ],
      type: :set
    ])
    :ok
  end

  describe "Ecto.Adapters.Schema#insert" do
    test "Repo#insert valid record with [on_conflict: :replace_all]" do
      case TestRepo.insert(%TestSchema{field: "field"}, on_conflict: :replace_all) do
        {:ok, %{id: id, field: "field"}} ->
          assert true
          {:atomic, [result]} = :mnesia.transaction(fn ->
            :mnesia.read(@table_name, id)
          end)
          {TestSchema, ^id, field, _, _} = result
          assert field == "field"
        _ -> assert false
      end

      :mnesia.clear_table(@table_name)
    end

    test "Repo#insert valid record with [on_conflict: :replace_all] and returning opt" do
      case TestRepo.insert(%TestSchema{field: "field"}, on_conflict: :replace_all, returning: [:id, :field]) do
        {:ok, %{id: id, field: "field"}} ->
          assert true
          {:atomic, [result]} = :mnesia.transaction(fn ->
            :mnesia.read(@table_name, id)
          end)
          {TestSchema, ^id, field, _, _} = result
          assert field == "field"
        _ -> assert false
      end

      :mnesia.clear_table(@table_name)
    end
  end
end
