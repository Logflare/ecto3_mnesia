defmodule Ecto.Adapters.Mnesia.Table do
  @spec record_field_index(
    attribute :: atom(),
    table_name :: atom()
  ) :: field_index :: integer()
  def record_field_index(attribute, table_name) do
    Enum.find_index(
      attributes(table_name),
      fn (e) -> e == attribute end
    )
  end

  @spec field_index(
    field :: atom(),
    table_name :: atom()
  ) :: field_index :: integer()
  def field_index(field, table_name) do
    record_field_index(field, table_name) + 1
  end

  @spec field_name(
    index :: integer(),
    table_name :: atom()
  ) :: field_name :: atom()
  def field_name(index, table_name) do
    Enum.at(attributes(table_name), index)
  end

  @spec attributes(table_name :: atom()) :: attributes :: list(atom())
  def attributes(table_name) do
    :mnesia.table_info(table_name, :attributes)
  end
end
