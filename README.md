[![pipeline status](https://gitlab.com/patatoid/ecto3_mnesia/badges/master/pipeline.svg)](https://gitlab.com/patatoid/ecto3_mnesia/-/commits/master)
[![coverage report](https://gitlab.com/patatoid/ecto3_mnesia/badges/master/coverage.svg)](https://gitlab.com/patatoid/ecto3_mnesia/-/commits/master)
# Ecto Mnesia Adapter
This adapter brings the strength of Ecto providing validation, and persistance layer to interact to Mnesia databases.

Mnesia is Distributed Database Management System shipped with Erlang runtime. Be aware of strengths and weaknesses listed in [erlang documentation](https://erlang.org/doc/man/mnesia.html) before thinking about using it.


## What works
1. Queries
- [x] Basic all queries
- [x] Select queries
- [x] Simple where queries
- [x] and/or/in in where clauses
- [x] Bindings
- [ ] Fragments
- [x] Limit/Offset queries
- [x] Sort by one field
- [ ] Sort by multiple fields
- [x] One level joins
- [ ] Deeper joins

2. Writing operations
- [x] insert/insert_all
- [x] update/update_all
- [x] delete/delete_all
- [x] Auto incremented ids
- [x] Binary ids

Note: supports only on_conflict: :raise/:update_all

3. Associations
- [x] has_one associations
- [x] has_many associations
- [x] belongs_to associations
- [ ] many_to_many associations

4. Transactions
- [x] Create transactions
- [x] Rollback transactions

## Instalation
You can include ecto3_mnesia in your dependencies as follow:
```
  defp deps do
    ...
    {:ecto3_mnesia, "~> 0.2.0"},
    ...
  end
```
Then configure your application repository to use Mnesia adapter as follow:
```
# ./lib/my_app/repo.ex
defmodule MyApp.Repo do
  use Ecto.Repo,
    otp_app: :my_app,
    adapter: Ecto.Adapters.Mnesia
end
```

## Migrations
Migrations are not supported yet, you can use mnesia abilities to create tables in a script.
```
# ./priv/repo/mnesia_migration.exs
IO.inspect :mnesia.create_table(:table_name, [
  disc_copies: [node()],
  record_name: MyApp.Context.Schema,
  attributes: [:id, :field, :updated_at, :inserted_at],
  type: :set
])
```
Then run the script with mix `mix run ./priv/repo/mnesia_migration.exs`

Notice that the table before MUST be defined according to the corresponding schema
```
defmodule MyApp.Context.Schema do
  ...
  schema "table_name" do
    field :field, :string

    timestamps()
  end
  ...
end
```

## Tests
You can run the tests as any mix package running
```
git clone https://gitlab.com/patatoid/ecto3_mnesia.git
cd ecto3_mnesia
mix deps.get
mix test --trace
```

## Thanks
Many thanks to the contributors of ecto_sql and ecto_mnesia for their work on those topics that have inspired some of the design of this library

## Contributing
Contributions of any kind are welcome :)
