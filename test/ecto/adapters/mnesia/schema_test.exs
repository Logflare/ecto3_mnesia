defmodule Ecto.Adapters.Mnesia.SchemaIntegrationTest do
  use ExUnit.Case, async: false

  alias EctoMnesia.TestRepo
  alias Ecto.Adapters.Mnesia

  @table_name __MODULE__.Table
  @binary_id_table_name __MODULE__.BinaryIdTable

  defmodule TestSchema do
    use Ecto.Schema

    schema "#{Ecto.Adapters.Mnesia.SchemaIntegrationTest.Table}" do
      timestamps()

      field :field, :string
    end

    def changeset(%TestSchema{} = struct, params) do
      struct
      |> Ecto.Changeset.cast(params, [:field])
    end
  end

  defmodule BinaryIdTestSchema do
    use Ecto.Schema

    @primary_key {:id, :binary_id, autogenerate: true}
    schema "#{Ecto.Adapters.Mnesia.SchemaIntegrationTest.BinaryIdTable}" do
      timestamps()

      field :field, :string
    end

    def changeset(%TestSchema{} = struct, params) do
      struct
      |> Ecto.Changeset.cast(params, [:field])
    end
  end

  setup_all do
    ExUnit.CaptureLog.capture_log fn -> Mnesia.storage_up(nodes: [node()]) end
    Mnesia.ensure_all_started([], :permanent)
    {:ok, _repo} = TestRepo.start_link()

    :mnesia.create_table(@table_name, [
      ram_copies: [node()],
      record_name: TestSchema,
      attributes: [:id, :field, :inserted_at, :updated_at],
      storage_properties: [ ets: [:compressed] ],
      type: :set
    ])
    :mnesia.create_table(@binary_id_table_name, [
      ram_copies: [node()],
      record_name: BinaryIdTestSchema,
      attributes: [:id, :field, :inserted_at, :updated_at],
      storage_properties: [ ets: [:compressed] ],
      type: :set
    ])

    :mnesia.wait_for_tables([@table_name, @binary_id_table_name], 1000)
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

    test "Repo#insert valid record with binary id, [on_conflict: :replace_all]" do
      case TestRepo.insert(%BinaryIdTestSchema{field: "field"}, on_conflict: :replace_all) do
        {:ok, %{id: id, field: "field"}} ->
          assert true


          %BinaryIdTestSchema{id: id, field: field} = TestRepo.get!(BinaryIdTestSchema, id)
          assert id =~ ~r([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})
          assert field == "field"
        _ -> assert false
      end

      :mnesia.clear_table(@binary_id_table_name)
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

  describe "Ecto.Adapters.Schema#insert_all" do
    test "Repo#insert_all valid records with [on_conflict: :replace_all]" do
      case TestRepo.insert_all(
        TestSchema,
        [%{field: "field 1"}, %{field: "field 2"}],
        on_conflict: :replace_all,
        returning: [:id]
      ) do
        {count, _records} ->
          assert count == 2

          {:atomic, results} = :mnesia.transaction(fn ->
            :mnesia.foldl(fn (record, acc) -> [record|acc] end, [], @table_name)
          end)

          assert Enum.all?(results, fn
            ({TestSchema, _, "field 1", _, _}) -> true
            ({TestSchema, _, "field 2", _, _}) -> true
            (_) -> false
          end)
        _ -> assert false
      end

      :mnesia.clear_table(@table_name)
    end

    test "Repo#insert_all valid records with [on_conflict: :replace_all] and returning opt" do
      case TestRepo.insert_all(
        TestSchema,
        [%{field: "field 1"}, %{field: "field 2"}],
        on_conflict: :replace_all,
        returning: [:id]
      ) do
        {count, records} ->
          assert count == 2
          {:atomic, results} = :mnesia.transaction(fn ->
            Enum.map(records, fn (%{id: id}) ->
              :mnesia.read(@table_name, id)
            end)
          end)

          assert Enum.all?(results, fn
            ([{TestSchema, _, "field 1", _, _}]) -> true
            ([{TestSchema, _, "field 2", _, _}]) -> true
            (_) -> false
          end)
        _ -> assert false
      end

      :mnesia.clear_table(@table_name)
    end

    test "Repo#insert_all valid records with binary ids, [on_conflict: :replace_all] and returning opt" do
      case TestRepo.insert_all(
        BinaryIdTestSchema,
        [%{field: "field 1"}, %{field: "field 2"}],
        on_conflict: :replace_all,
        returning: [:id, :field]
      ) do
        {count, records} ->
          assert count == 2
          assert length(records) == 2

          Enum.map(records, fn
            (%{id: id, field: "field 1"}) ->
              assert TestRepo.get!(BinaryIdTestSchema, id)
              assert id =~ ~r([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})
            (%{id: id, field: "field 2"}) ->
              assert TestRepo.get!(BinaryIdTestSchema, id)
              assert id =~ ~r([0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12})
          end)
        _ -> assert false
      end

      :mnesia.clear_table(@table_name)
    end
  end

  describe "Ecto.Adapters.Schema#update" do
    setup do
      {:atomic, _} = :mnesia.transaction(fn ->
        :mnesia.write(@table_name, {TestSchema, 1, "field", nil, nil}, :write)
      end)
      record = TestRepo.get(TestSchema, 1)
      {:ok, record: record}
    end

    test "Repo#update valid record with [on_conflict: :replace_all]", %{record: record} do
      id = record.id
      changeset = TestSchema.changeset(record, %{field: "field updated"})

      case TestRepo.update(changeset) do
        {:ok, %TestSchema{id: ^id, field: "field updated"}} ->
          case :mnesia.transaction(fn ->
            :mnesia.read(@table_name, id)
          end) do
            {:atomic, [{TestSchema, ^id, "field updated", _, _}]} -> assert true
            e -> assert false == e
          end
        _ -> assert false
      end

      :mnesia.clear_table(@table_name)
    end

    test "Repo#update non existing record with [on_conflict: :replace_all]", %{record: record} do
      changeset = TestSchema.changeset(%{record|id: 3}, %{field: "field updated"})

      assert_raise Ecto.StaleEntryError, fn ->
        TestRepo.update(changeset)
      end

      :mnesia.clear_table(@table_name)
    end
  end

  describe "Ecto.Adapters.Schema#delete" do
    setup do
      {:atomic, _} = :mnesia.transaction(fn ->
        :mnesia.write(@table_name, {TestSchema, 1, "field", nil, nil}, :write)
      end)
      record = TestRepo.get(TestSchema, 1)
      {:ok, record: record}
    end

    test "Repo#delete an existing record", %{record: record} do
      case TestRepo.delete(record) do
        {:ok, %TestSchema{id: 1, field: "field"}} ->
          case :mnesia.transaction(fn ->
            :mnesia.read(@table_name, 1)
          end) do
            {:atomic, []} -> assert true
            _ -> assert false
          end
        _ -> assert false
      end

      :mnesia.clear_table(@table_name)
    end

    test "Repo#delete a non existing record", %{record: record} do
      assert_raise Ecto.StaleEntryError, fn ->
        TestRepo.delete(%{record|id: 2})
      end

      :mnesia.clear_table(@table_name)
    end
  end
end
