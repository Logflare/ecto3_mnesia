IO.inspect :mnesia.create_table(:binary_id_table, [
  ram_copies: [[:nonode@nohost]],
  record_name: BinaryIdTestSchema,
  attributes: [[:id, :inserted_at, :updated_at, :field]],
  type: :set
])
