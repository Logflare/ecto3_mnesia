defmodule EctoMnesia.MixProject do
  use Mix.Project

  def project do
    [
      app: :ecto_mnesia,
      version: "0.1.0",
      elixir: "~> 1.9",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:ecto, "~> 3.3"},
    ]
  end
end
