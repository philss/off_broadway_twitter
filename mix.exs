defmodule OffBroadwayTwitter.MixProject do
  use Mix.Project

  def project do
    [
      app: :off_broadway_twitter,
      version: "0.1.0",
      elixir: "~> 1.11",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {OffBroadwayTwitter.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:broadway, "~> 1.0"},
      {:castore, "~> 0.1"},
      {:mint, "~> 1.0"},
      {:jason, "~> 1.2"}
    ]
  end
end
