defmodule DummyRepo do
  use Ecto.Repo,
    otp_app: :ecto3_mnesia,
    adapter: Ecto.Adapters.Mnesia
end

defmodule Mix.Tasks.Ecto3Mnesia do
  defp start_mnesia(repos) do
    Application.ensure_all_started(:ecto3_mnesia)
    Enum.each(repos, & &1.start_link())
    :ok
  end

  if Mix.env() == :dev do
    def migration_dir_not_set() do
      "./priv/example_mnesia_migrations"
    end
  else
    def migration_dir_not_set() do
      IO.puts(~s(Please set a migration directory in config.exs
          config Ecto3Mnesia,
          mig_dir: "./priv/mnesia_migrations"))
    end
  end

  def setup(opts) do
    repo = Mix.Ecto.parse_repo(opts)
    start_mnesia(repo)

    case Application.fetch_env(Ecto3Mnesia, :mig_dir) do
      {:ok, mig_dir} ->
        File.ls!(mig_dir)
        |> Enum.map(&Path.join(mig_dir, &1))
        |> Enum.filter(&String.ends_with?(&1, ".exs"))
        |> Enum.each(&Code.eval_file(&1))

      :error ->
        migration_dir_not_set()
    end
  end

  def run(["setup" | opts]) do
    setup(opts)
  end

  def run(["reset" | opts]) do
    IO.inspect(:mnesia.delete_schema([node()]))
    setup(opts)
  end

  def run(["gen", schema_name | opts]) do
    {parsed_opts, _, _} =
      OptionParser.parse(opts, strict: [table_type: :string], aliases: [t: :table_type])

    table_type = Keyword.get(parsed_opts, :table_type, :set)
    schema = String.to_atom("Elixir.#{schema_name}")

    case Application.fetch_env(Ecto3Mnesia, :mig_dir) do
      {:ok, mig_dir} ->
        generate_migration(schema, table_type, mig_dir)

      :error ->
        generate_migration(schema, table_type, migration_dir_not_set())
    end
  end

  defp validate_schema(module) do
    if {:__schema__, 1} in module.__info__(:functions) do
      :ok
    else
      {:error, :not_a_schema}
    end
  end

  defp validate_table_type(table_type) when table_type in ~w(set ordered_set bag), do: :ok
  defp validate_table_type(_), do: {:error, :invalid_table_type}

  defp generate_migration(schema, table_type, mig_dir) do
    with {:module, module} <- Code.ensure_loaded(schema),
         :ok <- validate_table_type(table_type),
         :ok <- validate_schema(module) do
      table_name = schema.__schema__(:source)
      fields = schema.__schema__(:fields)
      nodes = Application.get_env(Ecto3Mnesia, :nodes, [node()])

      migration =
        File.read!(Path.join(:code.priv_dir(:ecto3_mnesia), "migration_template.exs"))
        |> String.replace("FIELDS", "#{inspect(fields)}")
        |> String.replace("SCHEMA_NAME", "#{inspect(schema)}")
        |> String.replace("TABLE_NAME", ":#{table_name}")
        |> String.replace("TABLE_TYPE", ":#{table_type}")
        |> String.replace("NODES", "#{inspect(nodes)}")

      File.mkdir_p!(mig_dir)
      migration_path = Path.join(mig_dir, "create_#{table_name}.exs")
      File.write(migration_path, migration)
    else
      {:error, error} ->
        error
        |> Atom.to_string()
        |> IO.puts()
    end
  end
end
