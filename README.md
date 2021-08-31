# Off Broadway Twitter

A naive implementation of a Broadway producer for the Twitter stream V2.
This does not have ACK. It just prints the tweets.

This is an example app for the text I wrote for Dashbit's
blog: [Building a custom Broadway producer for the Twitter stream API](https://dashbit.co/blog/building-a-custom-broadway-producer-for-the-twitter-stream-api).

## Running

In order to run this app, first you need to create an account on
[Twitter's Developer area](https://developer.twitter.com/).

After that, create an app that has access to the Stream API V2.
You will need to get a "Bearer Token".

With the token in hand, you can run the app with:

    $ TWITTER_BEARER_TOKEN=your-token-here iex -S mix

Check more about Broadway in our [Broadway website](https://elixir-broadway.org).
