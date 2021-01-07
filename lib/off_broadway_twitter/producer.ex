defmodule OffBroadwayTwitter.Producer do
  use GenStage

  alias Broadway.Producer
  alias Broadway.Message

  alias Mint.HTTP2

  @behaviour Producer
  @twitter_stream_url_v2 "https://api.twitter.com/2/tweets/sample/stream"
  @connect_in_ms 2_000
  @reconnect_in_ms 1_000

  @impl true
  def init(opts) do
    uri = URI.parse(@twitter_stream_url_v2)
    token = Keyword.fetch!(opts, :twitter_bearer_token)

    timer = schedule_connection(@connect_in_ms)

    {:producer,
     %{
       demand: 0,
       tweets: [],
       request_ref: nil,
       last_message: nil,
       conn: nil,
       token: token,
       connection_timer: timer,
       uri: uri
     }}
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
    {:ok, conn} = HTTP2.connect(:https, state.uri.host, state.uri.port)

    {:ok, conn, request_ref} =
      HTTP2.request(
        conn,
        "GET",
        state.uri.path,
        [{"Authorization", "Bearer #{state.token}"}],
        nil
      )

    {:noreply, [], %{state | request_ref: request_ref, conn: conn, connection_timer: nil}}
  end

  @impl true
  def handle_info({tag, _socket, _data} = message, state) when tag in [:tcp, :ssl] do
    conn = state.conn

    case HTTP2.stream(conn, message) do
      {:ok, conn, resp} ->
        process_responses(resp, %{state | conn: conn})

      {:error, conn, %Mint.HTTPError{reason: {:server_closed_connection, :refused_stream, _}}, _} ->
        timer = schedule_connection(@reconnect_in_ms)

        {:noreply, [], %{state | conn: conn, connection_timer: timer}}

      {:error, conn, %{reason: reason}, _} when reason in [:closed, :einval] ->
        # I got :einval once, but I suppose I was doing something wrong
        timer = schedule_connection(@connect_in_ms)

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
    responses
    |> Enum.reduce(state, fn response, state ->
      ref = state.request_ref

      case response do
        {:data, ^ref, tweet} ->
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

              %{state | tweets: [message | state.tweets]}

            {:error, _} ->
              IO.puts("error decoding")

              state
          end

        {:done, ^ref} ->
          state

        {_, _ref, _other} ->
          state
      end
    end)
    |> handle_received_messages()
  end

  @impl true
  def handle_demand(demand, state) do
    handle_received_messages(%{state | demand: state.demand + demand})
  end

  # We are are dispatching events as they arrive.
  # If there is no Consumer or demand is low, then GenStage
  # will buffer the messages internally.
  # We could have an internal control, but is not necessary
  # for this example.
  defp handle_received_messages(state) do
    len = length(state.tweets)

    {:noreply, Enum.reverse(state.tweets),
     %{state | tweets: [], demand: Enum.max([0, state.demand - len])}}
  end
end
