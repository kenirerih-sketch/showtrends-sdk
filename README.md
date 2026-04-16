# showtrends-sdk

SDKs for the Showtrends web API.

The first SDK lives in `go/` and provides a small batching client for posting counter and value stats to a Showtrends server.
The Go client takes the server URL and namespace up front, then sends stats by name with simple `Count`, `CountOne`, and `Value` methods.
