defmodule OffBroadwayTwitter.Producer do
  use GenStage

  alias Broadway.Producer
  alias Broadway.Message
  alias OffBroadwayTwitter.TwitterFinch

  @behaviour Producer
  @twitter_stream_url_v2 "https://api.twitter.com/2/tweets/sample/stream"

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    :ok = connect(opts)

    tweets_queue = :queue.new()

    {:producer, %{demand: 0, queue: tweets_queue, size: 0}}
  end

  @impl true
  def handle_demand(demand, state) do
    IO.inspect(state.size, label: "size")
    IO.inspect(state.demand + demand, label: "demand")

    handle_received_messages(%{state | demand: state.demand + demand})
  end

  defp handle_received_messages(state) do
    amount_to_fetch =
      if state.size >= state.demand do
        state.demand
      else
        0
      end

    {tweets, new_queue} = get_tweets(state.queue, amount_to_fetch, [])

    {:noreply, tweets,
     %{
       state
       | queue: new_queue,
         size: state.size - amount_to_fetch,
         demand: state.demand - amount_to_fetch
     }}
  end

  defp get_tweets(tweets_queue, 0, tweets), do: {Enum.reverse(tweets), tweets_queue}

  defp get_tweets(tweets_queue, demand, tweets) do
    {{:value, tweet}, queue} = :queue.out(tweets_queue)

    get_tweets(queue, demand - 1, [tweet | tweets])
  end

  @impl true
  def handle_info({:data, tweet}, state) do
    case Jason.decode(tweet) do
      {:ok, decoded} ->
        data = Map.fetch!(decoded, "data")
        meta = Map.delete(data, "text")
        text = Map.fetch!(data, "text")

        message = %Message{
          data: text,
          metadata: meta,
          acknowledger: {Broadway.NoopAcknowledger, nil, nil}
        }

        handle_received_messages(%{
          state
          | queue: :queue.in(message, state.queue),
            size: state.size + 1
        })

      {:error, _} ->
        IO.puts("error decoding")
        handle_received_messages(state)
    end
  end

  defp connect(opts) do
    self_pid = self()
    token = Keyword.fetch!(opts, :twitter_bearer_token)
    request = Finch.build(:get, @twitter_stream_url_v2, [{"Authorization", "Bearer #{token}"}])

    spawn_link(fn ->
      Finch.stream(request, TwitterFinch, nil, fn
        {:data, data}, _ ->
          send(self_pid, {:data, data})

        _, _ ->
          nil
      end)
    end)

    :ok
  end
end
