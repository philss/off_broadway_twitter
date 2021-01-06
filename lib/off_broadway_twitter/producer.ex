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

    IO.puts("init => #{now()}")
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
    IO.puts("connected => #{now()}")

    {:ok, conn, request_ref} =
      HTTP2.request(
        conn,
        "GET",
        state.uri.path,
        [{"Authorization", "Bearer #{state.token}"}],
        nil
      )

    IO.puts("requested => #{now()}")

    #send(self(), {:process_stream, conn})

    IO.puts("started => #{now()}")

    {:noreply, [], %{state | request_ref: request_ref, conn: conn}}
  end

  @impl true
  def handle_info({:process_stream, conn}, %{last_message: nil} = state) do
    IO.puts("nil last message, #{now()}")
    send(self(), {:process_stream, conn})

    {:noreply, [], state}
  end

  @impl true
  def handle_info({:process_stream, conn}, state) do
    case HTTP2.stream(conn, state.last_message) do
      {:ok, conn, resp} ->
        process_responses(resp, %{state | conn: conn})

      {:error, conn, %Mint.HTTPError{reason: {:server_closed_connection, :refused_stream, _}}, _} ->
        IO.puts("disconnecting => #{now()}")
        Mint.HTTP.close(conn)
        IO.puts("starting again => #{now()}")
        Process.send_after(self(), :start_stream, 100)

        {:noreply, [], %{state | conn: conn}}

      {:error, _conn, %{reason: reason}, _} when reason in [:closed, :einval] ->
        IO.puts("ERROR unknown => #{reason}, #{now()}")
        IO.puts("starting again => #{now()}")
        Process.send_after(self(), :start_stream, 2_000)

        {:noreply, [], %{state | conn: conn}}

      {:error, other} ->
        IO.inspect(other, label: "stopping stream => #{now()}")

        {:stop, :stream_stopped, state}

      message ->
        IO.inspect(message, label: "unknown stream result => #{now()}")
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
    IO.puts("closed SSL/TCP => #{now()}")

    {:noreply, [], %{state | last_message: message}}
  end

  @impl true
  def handle_info(message, state) do
    IO.inspect(message, label: "unknown")

    IO.puts("sending reconnecting => #{now()}")
    Process.send_after(self(), :start_stream, 1_000)

    {:noreply, [], state}
  end

  defp now do
    DateTime.utc_now()
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
            IO.puts("reconnecting after done, #{now()}")
            # Process.send_after(self(), :start_stream, 1_000)
            state

          {_, _ref, other} ->
            IO.inspect(other, label: "other things")
            state
        end
      end)

    handle_received_messages(state)
  end

  @impl true
  def handle_demand(demand, state) do
    #IO.inspect(state.size, label: "size")
    #IO.inspect(state.demand + demand, label: "demand")

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
