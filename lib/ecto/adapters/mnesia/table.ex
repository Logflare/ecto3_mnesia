defmodule Ecto.Adapters.Mnesia.Table do
  @spec attribute_index(
    attribute :: atom(),
    table_name :: atom()
  ) :: index :: integer()
  def attribute_index(attribute, table_name) do
    Enum.find_index(
      attributes(table_name),
      fn (e) -> e == attribute end
    )
  end

  @spec field_index(
    field :: atom(),
    table_name :: atom()
  ) :: index :: integer()
  def field_index(field, table_name) do
    attribute_index(field, table_name) + 1
  end

  @spec attributes(table_name :: atom()) :: attributes :: list(atom())
  def attributes(table_name) do
    :mnesia.table_info(table_name, :attributes)
  end
end
