defmodule EctoMnesia.MixProject do
  use Mix.Project

  def project do
    [
      name: "Ecto3 Mnesia",
      app: :ecto3_mnesia,
      version: "0.2.1",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [
        plt_add_apps: [:mnesia],
      ],
      source_url: "https://gitlab.com/patatoid/ecto3_mnesia",
      description: description(),
      package: package(),
      docs: [
        main: "Ecto.Adapters.Mnesia",
        extras: ["README.md"]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ecto, "~> 3.0"},
      {:qlc, "~> 1.0"},
      {:ex_doc, "~> 0.21", only: :dev, runtime: false},
      {:dialyxir, "~> 1.0.0-rc.7", only: [:dev], runtime: false}
    ]
  end

  defp package do
    %{
      name: "ecto3_mnesia",
      licenses: ["MIT"],
      links: %{
        "Gitlab" => "https://gitlab.com/patatoid/ecto3_mnesia"
      }
    }
  end

  defp description do
    """
    Mnesia adapter for Ecto 3
    """
  end
end
