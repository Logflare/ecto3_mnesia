defmodule BinaryIdTestSchema do
  use Ecto.Schema
  
  @primary_key {:id, :binary_id, autogenerate: true}
  schema "binary_id_table" do
    timestamps()
    
    field(:field, :string)
  end
end
