defmodule OffBroadwayTwitter.Producer do
  use GenStage

  alias Broadway.Producer
  alias Broadway.Message

  alias Mint.HTTP2

  @behaviour Producer
  @twitter_stream_url_v2 "https://api.twitter.com/2/tweets/sample/stream"

  @impl true
  def init(opts) do
    {:ok, conn, uri} = connect(opts)

    tweets_queue = :queue.new()

    token = Keyword.fetch!(opts, :twitter_bearer_token)
    Process.send_after(self(), {:first_request, token: token, conn: conn}, 2_000)

    {:producer, %{demand: 0, queue: tweets_queue, size: 0, uri: uri, last_message: nil}}
  end

  @impl true
  def handle_info({:first_request, opts}, state) do
    token = Keyword.fetch!(opts, :token)
    conn = Keyword.fetch!(opts, :conn)
    IO.puts("SECOND")

    {:ok, conn, _request_ref} =
      HTTP2.request(
        conn,
        "GET",
        state.uri.path,
        [{"Authorization", "Bearer #{token}"}],
        :stream
      )

    send(self(), {:process_stream, conn})

    {:noreply, [], state}
  end

  @impl true
  def handle_info({:process_stream, conn}, state) do
    IO.puts("THIRD -> multiple")
    case HTTP2.stream(conn, state.last_message) do
      {:ok, %HTTP2{} = conn, resp} ->
        send(self(), {:process_stream, conn})
        {:noreply, [], state}

      other ->
        IO.inspect(other, label: "stopping stream")
        {:stop, :stream_stopped, state}
    end
  end

  @impl true
  def handle_info({tag, _socket, _data} = message, state) when tag in [:tcp, :ssl] do
    IO.puts("FORTH")
    IO.inspect(message, label: "AAAAAA: last message")
    {:noreply, [], %{state | last_message: message}}
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

  @impl true
  def handle_info(message, state) do
    IO.inspect(message, label: "BBBBBBBBBBBBBBBBB unknown")

    {:noreply, [], state}
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

  defp connect(opts) do
    uri = URI.parse(@twitter_stream_url_v2)

    {:ok, %HTTP2{} = conn} = HTTP2.connect(:https, uri.host, uri.port)

    IO.puts("FIRST")

    {:ok, conn, uri}
  end
end
