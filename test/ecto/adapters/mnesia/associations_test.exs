defmodule Ecto.Adapters.MnesiaAssociationsIntegrationTest do
  use ExUnit.Case, async: false

  alias EctoMnesia.TestRepo
  alias Ecto.Adapters.Mnesia

  @has_many_table_name __MODULE__.HasMany
  @belongs_to_table_name __MODULE__.BelongsTo

  defmodule BelongsToSchema do
    use Ecto.Schema

    schema "#{Ecto.Adapters.MnesiaAssociationsIntegrationTest.BelongsTo}" do
      field :field, :string

      belongs_to :has_many, Ecto.Adapters.MnesiaAssociationsIntegrationTest.HasManySchema
    end
  end

  defmodule HasManySchema do
    use Ecto.Schema

    schema "#{Ecto.Adapters.MnesiaAssociationsIntegrationTest.HasMany}" do
      field :field, :string

      has_many :belongs_tos, Ecto.Adapters.MnesiaAssociationsIntegrationTest.BelongsToSchema,
        foreign_key: :has_many_id
    end
  end

  setup_all do
    ExUnit.CaptureLog.capture_log fn -> Mnesia.storage_up(nodes: [node()]) end
    Mnesia.ensure_all_started([], :permanent)
    {:ok, _repo} = TestRepo.start_link()

    :mnesia.create_table(@has_many_table_name, [
      ram_copies: [node()],
      record_name: HasManySchema,
      attributes: [:id, :field],
      storage_properties: [
        ets: [:compressed]
      ],
      type: :ordered_set
    ])

    :mnesia.create_table(@belongs_to_table_name, [
      ram_copies: [node()],
      record_name: BelongsToSchema,
      attributes: [:id, :field, :has_many_id],
      storage_properties: [
        ets: [:compressed]
      ],
      type: :ordered_set
    ])
    :mnesia.wait_for_tables([@has_many_table_name, @belongs_to_table_name], 1000)
  end

  test "preload has_many association" do
    :mnesia.transaction fn ->
      :mnesia.write(@has_many_table_name, {HasManySchema, 1, "has many"}, :write)
      :mnesia.write(@belongs_to_table_name, {BelongsToSchema, 1, "belongs to", 1}, :write)
    end

    case TestRepo.get(HasManySchema, 1) |> TestRepo.preload(:belongs_tos) do
      %HasManySchema{belongs_tos: belongs_tos} ->
        assert belongs_tos == [TestRepo.get(BelongsToSchema, 1)]
      _ -> assert false
    end
  end

  test "preload belongs_to association" do
    :mnesia.transaction fn ->
      :mnesia.write(@has_many_table_name, {HasManySchema, 1, "has many"}, :write)
      :mnesia.write(@belongs_to_table_name, {BelongsToSchema, 1, "belongs to", 1}, :write)
    end

    case TestRepo.get(BelongsToSchema, 1) |> TestRepo.preload(:has_many) do
      %BelongsToSchema{has_many: has_many} ->
        assert has_many == TestRepo.get(HasManySchema, 1)
      _ -> assert false
    end
  end
end
