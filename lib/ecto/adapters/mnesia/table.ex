defmodule Ecto.Adapters.Mnesia.Table do
  @spec field_index(
    field :: atom(),
    table_name :: atom()
  ) :: index :: integer()
  def field_index(field, table_name) do
    Enum.find_index(
      attributes(table_name),
      fn (attribute) -> attribute == field end
    )
  end

  @spec attributes(table_name :: atom()) :: attributes :: list(atom())
  def attributes(table_name) do
    :mnesia.table_info(table_name, :attributes)
  end
end
