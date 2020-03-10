defmodule EctoMnesia.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto3_mnesia,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [
        plt_add_apps: [:mnesia],
      ],
      source_url: "https://gitlab.com/patatoid/ecto3_mnesia",
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
end
