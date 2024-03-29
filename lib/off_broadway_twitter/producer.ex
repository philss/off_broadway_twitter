defmodule OffBroadwayTwitter.Producer do
  use GenStage

  alias Broadway.Producer
  alias Broadway.Message

  alias Mint.HTTP2

  @behaviour Producer
  @twitter_stream_url_v2 "https://api.twitter.com/2/tweets/sample/stream"
  @reconnect_in_ms 1_000

  @impl true
  def init(opts) do
    uri = URI.parse(@twitter_stream_url_v2)
    token = Keyword.fetch!(opts, :twitter_bearer_token)

    state =
      connect_to_stream(%{
        request_ref: nil,
        last_message: nil,
        conn: nil,
        token: token,
        connection_timer: nil,
        uri: uri
      })

    {:producer, state}
  end

  defp connect_to_stream(state) do
    {:ok, conn} = HTTP2.connect(:https, state.uri.host, state.uri.port)

    {:ok, conn, request_ref} =
      HTTP2.request(
        conn,
        "GET",
        state.uri.path,
        [{"Authorization", "Bearer #{state.token}"}],
        nil
      )

    %{state | request_ref: request_ref, conn: conn, connection_timer: nil}
  end

  defp schedule_connection(interval) do
    Process.send_after(self(), :connect_to_stream, interval)
  end

  @impl Producer
  def prepare_for_draining(%{connection_timer: connection_timer} = state) do
    connection_timer && Process.cancel_timer(connection_timer)
    {:noreply, [], %{state | connection_timer: nil}}
  end

  @impl true
  def handle_info(:connect_to_stream, state) do
    {:noreply, [], connect_to_stream(state)}
  end

  @impl true
  def handle_info({tag, _socket, _data} = message, state) when tag in [:tcp, :ssl] do
    conn = state.conn

    case HTTP2.stream(conn, message) do
      {:ok, conn, resp} ->
        process_responses(resp, %{state | conn: conn})

      {:error, conn, %error{}, _} when error in [Mint.HTTPError, Mint.TransportError] ->
        timer = schedule_connection(@reconnect_in_ms)

        {:noreply, [], %{state | conn: conn, connection_timer: timer}}

      {:error, _other} ->
        {:stop, :stream_stopped_due_unknown_error, state}
    end
  end

  @impl true
  def handle_info({tag, _socket} = message, state) when tag in [:tcp_closed, :ssl_closed] do
    {:noreply, [], %{state | last_message: message}}
  end

  defp process_responses(responses, state) do
    ref = state.request_ref

    tweets =
      Enum.flat_map(responses, fn response ->
        case response do
          {:data, ^ref, tweet} ->
            decode_tweet(tweet)

          {:done, ^ref} ->
            []

          {_, _ref, _other} ->
            []
        end
      end)

    {:noreply, Enum.reverse(tweets), state}
  end

  defp decode_tweet(tweet) do
    case Jason.decode(tweet) do
      {:ok, %{"data" => data}} ->
        meta = Map.delete(data, "text")
        text = Map.fetch!(data, "text")

        [
          %Message{
            data: text,
            metadata: meta,
            acknowledger: {Broadway.NoopAcknowledger, nil, nil}
          }
        ]

      {:error, _} ->
        IO.puts("error decoding")

        []
    end
  end

  # We are are dispatching events as they arrive.
  # If there is no consumer or demand is low, then GenStage
  # will buffer the messages internally.
  #
  # For this scenario it's not necessary to have
  # an internal control, since Twitter is a firehouse
  # of events without back-pressure. We only ignore
  # the Tweets that doesn't fit the buffer.
  @impl true
  def handle_demand(_demand, state) do
    {:noreply, [], state}
  end
end
