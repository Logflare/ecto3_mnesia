defmodule EctoMnesia.TestRepo do
  use Ecto.Repo,
    otp_app: :ecto_mnesia,
    adapter: Ecto.Adapters.Mnesia
end
