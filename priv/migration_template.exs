IO.inspect :mnesia.create_table(TABLE_NAME, [
  ram_copies: [NODES],
  record_name: SCHEMA_NAME,
  attributes: [FIELDS],
  type: TABLE_TYPE
])
