defmodule OffBroadwayTwitter.Consumer do
  use Broadway

  alias Broadway.Message

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {OffBroadwayTwitter.Producer, opts},
        concurrency: 1
      ],
      processors: [
        default: [concurrency: 50]
      ],
      batchers: [
        default: [batch_size: 20, batch_timeout: 2000]
      ]
    )
  end

  @impl true
  def handle_message(_, %Message{data: data} = message, _) do
    Process.sleep(500)

    message
    |> Message.update_data(fn data -> String.upcase(data) end)
  end

  @impl true
  def handle_batch(_, messages, _, _) do
    #list = messages |> Enum.map(fn e -> e.data end)
    #IO.inspect(list, label: "Got batch")
    messages
  end
end
