defmodule RustPipe.MixProject do
  use Mix.Project

  def project do
    [
      app: :rust_pipe,
      version: "0.1.0",
      elixir: "~> 1.16",
      description: "Elixir worker SDK for rust-pipe — receive typed tasks from a Rust dispatcher",
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:websockex, "~> 0.4.3"},
      {:jason, "~> 1.4"}
    ]
  end

  defp package do
    [
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/albyte-ai/rust-pipe"}
    ]
  end
end
