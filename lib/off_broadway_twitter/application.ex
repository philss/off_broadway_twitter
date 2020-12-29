defmodule OffBroadwayTwitter.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Starts a worker by calling: OffBroadwayTwitter.Worker.start_link(arg)
      # {OffBroadwayTwitter.Worker, arg}
      {Finch,
       name: OffBroadwayTwitter.TwitterFinch,
       pools: %{
         default: [size: 1]
       }},
      {OffBroadwayTwitter.Consumer,
       twitter_bearer_token: System.fetch_env!("TWITTER_BEARER_TOKEN")}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: OffBroadwayTwitter.Supervisor]
    Supervisor.start_link(children, opts)
  end
end