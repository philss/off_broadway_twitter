defmodule OffBroadwayTwitter.Producer do
  use GenStage

  alias Broadway.Producer
  alias Broadway.Message

  alias Mint.HTTP2

  @behaviour Producer
  @twitter_stream_url_v2 "https://api.twitter.com/2/tweets/sample/stream"

  @impl true
  def init(opts) do
    tweets_queue = :queue.new()

    uri = URI.parse(@twitter_stream_url_v2)
    token = Keyword.fetch!(opts, :twitter_bearer_token)

    Process.send_after(self(), :start_stream, 2_000)

    {:producer,
     %{
       demand: 0,
       queue: tweets_queue,
       size: 0,
       request_ref: nil,
       last_message: nil,
       conn: nil,
       token: token,
       uri: uri
     }}
  end

  @impl true
  def handle_info(:start_stream, state) do
    {:ok, conn} = HTTP2.connect(:https, state.uri.host, state.uri.port)

    {:ok, conn, request_ref} =
      HTTP2.request(
        conn,
        "GET",
        state.uri.path,
        [{"Authorization", "Bearer #{state.token}"}],
        nil
      )

    {:noreply, [], %{state | request_ref: request_ref, conn: conn}}
  end

  @impl true
  def handle_info({:process_stream, conn}, state) do
    case HTTP2.stream(conn, state.last_message) do
      {:ok, conn, resp} ->
        process_responses(resp, %{state | conn: conn})

      {:error, conn, %Mint.HTTPError{reason: {:server_closed_connection, :refused_stream, _}}, _} ->
        Mint.HTTP.close(conn)
        Process.send_after(self(), :start_stream, 100)

        {:noreply, [], %{state | conn: conn}}

      {:error, _conn, %{reason: reason}, _} when reason in [:closed, :einval] ->
        Process.send_after(self(), :start_stream, 2_000)

        {:noreply, [], %{state | conn: conn}}

      {:error, other} ->
        {:stop, :stream_stopped, state}

      message ->
        {:noreply, [], state}
    end
  end

  @impl true
  def handle_info({tag, _socket, _data} = message, state) when tag in [:tcp, :ssl] do
    if state.conn do
      send(self(), {:process_stream, state.conn})
    end

    {:noreply, [], %{state | last_message: message}}
  end

  @impl true
  def handle_info({tag, _socket} = message, state) when tag in [:tcp_closed, :ssl_closed] do
    {:noreply, [], %{state | last_message: message}}
  end

  @impl true
  def handle_info(message, state) do
    Process.send_after(self(), :start_stream, 1_000)

    {:noreply, [], state}
  end

  defp process_responses(responses, state) do
    state =
      Enum.reduce(responses, state, fn response, state ->
        case response do
          {:data, _ref, tweet} ->
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

                %{state | queue: :queue.in(message, state.queue), size: state.size + 1}

              {:error, _} ->
                IO.puts("error decoding")

                state
            end

          {:done, _ref} ->
            state

          {_, _ref, _other} ->
            state
        end
      end)

    handle_received_messages(state)
  end

  @impl true
  def handle_demand(demand, state) do
    handle_received_messages(%{state | demand: state.demand + demand})
  end

  defp handle_received_messages(state) do
    amount_to_fetch =
      if state.size >= state.demand do
        state.demand
      else
        state.size
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
end
