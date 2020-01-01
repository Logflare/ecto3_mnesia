defmodule Ecto.Adapters.MnesiaQueryableIntegrationTest do
  use ExUnit.Case, async: false

  import Ecto.Query, only: [from: 2]

  alias EctoMnesia.TestRepo
  alias Ecto.Adapters.Mnesia

  @table_name __MODULE__.Table

  defmodule TestSchema do
    use Ecto.Schema

    schema "#{Ecto.Adapters.MnesiaQueryableIntegrationTest.Table}" do
      field :field, :string
    end
  end

  setup_all do
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

  describe "Ecto.Adapter.Queryable#execute" do
    test "#all from one table with no query, no records" do
      TestRepo.transaction fn ->
        assert TestRepo.all(TestSchema) == []
      end
    end

    test "#all from one table with no query, records" do
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: 2, field: "field 2"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      case TestRepo.all(TestSchema) do
        [] -> assert false
        fetched_records ->
          Enum.map(records, fn (%{id: id, field: field}) ->
            assert Enum.any?(fetched_records,
              fn
                (%{id: ^id, field: ^field}) -> true
                (_) -> false
              end
            )
          end)
      end

      :mnesia.clear_table(@table_name)
    end

    test "#all from one table with basic select query, records" do
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: 2, field: "field 2"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      assert TestRepo.all(
        from(s in TestSchema, select: s.id)
      ) == Enum.map(records, fn (%{id: id}) -> id end)

      :mnesia.clear_table(@table_name)
    end

    test "#all from one table with multiple field select query, records" do
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: 2, field: "field 2"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      assert TestRepo.all(
        from(s in TestSchema, select: [s.id, s.field])
      ) == Enum.map(records, fn (%{id: id, field: field}) -> [id, field] end)

      :mnesia.clear_table(@table_name)
    end

    test "#all from one table with simple where query, records" do
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: 2, field: "field 2"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      case TestRepo.all(
        from(s in TestSchema, where: s.id == 1)
      ) do
        [%{id: 1, field: "field 1"}] -> assert true
        e -> assert e == false
      end

      :mnesia.clear_table(@table_name)
    end

    test "#all from one table with simple where query, many records" do
      {:atomic, _result} = :mnesia.transaction(fn ->
        :mnesia.write(@table_name, {TestSchema, 1, "field"}, :write)

        Stream.iterate(0, &(&1 + 1))
        |> Enum.take(10_000)
        |> Enum.map(fn (id) ->
          :mnesia.write(@table_name, {TestSchema, id, "field #{id}"}, :write)
        end)
      end)

      {time, records} = :timer.tc(TestRepo, :all, [
        from(s in TestSchema, where: s.field == "field 2")
      ])
      assert Enum.all?(records, fn
        (%{field: "field 2"}) -> true
        _ -> false
      end)
      assert time < 50_000

      :mnesia.clear_table(@table_name)
    end

    test "#all from one table with complex (and) where query, records" do
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: 2, field: "field 2"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      records = TestRepo.all(
        from(s in TestSchema, where: s.field == "field 2" and s.id == 2)
      )
      refute Enum.empty?(records)
      assert Enum.all?(records, fn
        (%{id: 2, field: "field 2"}) -> true
        _ -> false
      end)

      :mnesia.clear_table(@table_name)
    end

    test "#all from one table with complex (or) where query, records" do
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: 2, field: "field 2"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      records = TestRepo.all(
        from(s in TestSchema, where: s.field == "field 2" or s.id == 1)
      )
      refute Enum.empty?(records)
      assert Enum.all?(records, fn
        (%{field: "field 2"}) -> true
        (%{id: 1}) -> true
        _ -> false
      end)

      :mnesia.clear_table(@table_name)
    end

    test "#all from one table with complex (mixed and / or) where query, records" do
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: 2, field: "field 2"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      records = TestRepo.all(
        from(s in TestSchema, where: s.field == "field 2" and s.id == 2 or s.field == "field 1" and s.id == 1)
      )
      refute Enum.empty?(records)
      assert Enum.all?(records, fn
        (%{id: 2, field: "field 2"}) -> true
        (%{id: 1, field: "field 1"}) -> true
        _ -> false
      end)

      :mnesia.clear_table(@table_name)
    end

    test "#all from one table with complex (is_nil) where query, records" do
      records = [
        %TestSchema{id: 1, field: nil},
        %TestSchema{id: 2, field: "field 2"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      records = TestRepo.all(
        from(s in TestSchema, where: is_nil(s.field))
      )
      assert Enum.all?(records, fn
        (%{field: nil}) -> true
        _ -> false
      end)

      :mnesia.clear_table(@table_name)
    end

    test "#all from one table with complex (binding) where query, records" do
      id = 2
      field = "field 2"
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: id, field: field}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      records = TestRepo.all(
        from(s in TestSchema, where: s.field == ^field and s.id == ^id)
      )
      assert Enum.all?(records, fn
        (%{field: ^field}) -> true
        _ -> false
      end)

      :mnesia.clear_table(@table_name)
    end

    test "#all from one table with complex (in) where query, records" do
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: 2, field: "field 2"},
        %TestSchema{id: 3, field: "field 3"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      case TestRepo.all(
        from(s in TestSchema, where: s.id in [1, 2, 3])
      ) do
        [
          %{id: 1, field: "field 1"},
          %{id: 2, field: "field 2"},
          %{id: 3, field: "field 3"}
        ] -> assert true
        e -> assert e == false
      end

      :mnesia.clear_table(@table_name)
    end

    test "#all from one table with complex (in / binding) where query, records" do
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: 2, field: "field 2"},
        %TestSchema{id: 3, field: "field 3"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      ids = [1, 2, 3]
      id = 1
      case TestRepo.all(
        from(s in TestSchema, where: s.id == ^id or s.id in ^ids)
      ) do
        [
          %{id: 1, field: "field 1"},
          %{id: 2, field: "field 2"},
          %{id: 3, field: "field 3"}
        ] -> assert true
        e -> assert e == false
      end

      :mnesia.clear_table(@table_name)
    end

    test "#update_all from one table with simple where query, records" do
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: 2, field: "field 2"},
        %TestSchema{id: 3, field: "field 3"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      case TestRepo.update_all(
        from(s in TestSchema, where: s.id == 1 or s.id == 2),
        set: [field: "updated field"]
      ) do
        {count, records} ->
          assert count == 2
          assert Enum.all?(records, fn
            (%{field: "updated field"}) -> true
            _ -> false
          end)
        e -> assert e == false
      end

      :mnesia.clear_table(@table_name)
    end

    test "#delete_all from one table with simple where query, records" do
      records = [
        %TestSchema{id: 1, field: "field 1"},
        %TestSchema{id: 2, field: "field 2"},
        %TestSchema{id: 3, field: "field 3"}
      ]
      {:atomic, _result} = :mnesia.transaction(fn ->
        Enum.map(records, fn (%{id: id, field: field}) ->
          :mnesia.write(@table_name, {TestSchema, id, field}, :write)
        end)
      end)

      case TestRepo.delete_all(
        from(s in TestSchema, where: s.id == 1 or s.id == 2)
      ) do
        {count, nil} ->
          assert count == 2
          case :mnesia.transaction(fn ->
            :mnesia.foldl(fn (record, acc) -> [record|acc] end, [], @table_name)
          end) do
            {:atomic, [{_, 3, "field 3"}]} -> assert true
            _ -> assert false
          end
      end

      :mnesia.clear_table(@table_name)
    end
  end
end

