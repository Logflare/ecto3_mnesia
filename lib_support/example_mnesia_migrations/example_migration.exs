:mnesia.create_table(@binary_id_table_name,
  ram_copies: [node()],
  record_name: BinaryIdTestSchema,
  attributes: [:id, :field, :inserted_at, :updated_at],
  storage_properties: [ets: [:compressed]],
  type: :ordered_set
)
