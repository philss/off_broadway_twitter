defmodule OffBroadwayTwitter.Foo do
  alias Mint.HTTP2

  @twitter_stream_url_v2 "https://api.twitter.com/2/tweets/sample/stream"
  def start do
    uri = URI.parse(@twitter_stream_url_v2)
    token = System.fetch_env!("TWITTER_BEARER_TOKEN")

    IO.puts(DateTime.utc_now())
    {:ok, conn} = HTTP2.connect(:https, uri.host, uri.port)

    IO.puts(DateTime.utc_now())

    {:ok, conn, request_ref} =
      HTTP2.request(
        conn,
        "GET",
        uri.path,
        [{"Authorization", "Bearer #{token}"}],
        nil
      )

    IO.puts(DateTime.utc_now())
    IO.puts("starting listing")
    listen(conn, request_ref)
  end

  defp listen(%{state: :closed}, _ref), do: :closed

  defp listen(conn, ref) do
    last_message =
      receive do
        msg -> msg
      end

    case HTTP2.stream(conn, last_message) do
      {:ok, conn, responses} ->
        IO.puts(DateTime.utc_now())
        IO.inspect(responses)
        listen(conn, ref)

      {:error, conn, %Mint.HTTPError{reason: {:server_closed_connection, :refused_stream, _}}, _} ->
        IO.puts("disconnecting")
        Mint.HTTP.close(conn)
        IO.puts("starting again")
        start()

      {:error, _conn, %Mint.HTTPError{reason: :closed}, _} ->
        IO.puts("ERROR unknown")
        #IO.inspect(error)
        IO.puts(DateTime.utc_now())
        #Mint.HTTP.close(conn)
        IO.puts("starting again")
        Process.sleep(2_000)
        start()
    end
  end
end
