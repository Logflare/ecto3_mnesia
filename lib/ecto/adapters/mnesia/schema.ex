defmodule Ecto.Adapters.Mnesia.Schema do
  import Ecto.Adapters.Mnesia.Table, only: [
    attributes: 1,
    field_index: 2
  ]
  def from_mnesia(table_name, record) do
    Enum.reduce(
      attributes(table_name),
      struct(elem(record, 0)), # schema struct
      fn (attribute, struct) ->
        %{struct |
          attribute => elem(record, field_index(attribute, table_name) + 1)
        }
      end
    )
  end
end
