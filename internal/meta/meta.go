// Package meta names the message metadata keys that cross package
// boundaries. Metadata is a flat string map that travels with a message
// from the inbound adapter, through scripts (which may read and write it),
// into the delivery queue, and out to the outbound adapter. Keys therefore
// form a small protocol between components that never import each other,
// and this package is the one place that protocol is written down.
//
// Namespaces:
//
//   - source.*   what the inbound adapter knew about the message; read
//     by scripts, never interpreted by outbound adapters
//   - message.id, channel.id, destination.id   stamped by the engine on
//     every delivery so outbound adapters can name what they are sending
//   - replay.of   set on messages re-entering the pipeline via replay
//   - http.*   outbound routing hints that scripts set for http-sender
//
// Inbound and outbound keys never share a name: an http-listener feeding an
// http-sender must not replay the inbound request's path and method against
// the outbound base URL.
package meta

// Engine-stamped delivery context.
const (
	MessageID     = "message.id"
	ChannelID     = "channel.id"
	DestinationID = "destination.id"
	ReplayOf      = "replay.of"
)

// Inbound adapter context.
const (
	// SourceRemote is the peer address for network sources.
	SourceRemote = "source.remote"
	// SourceFile is the file name for the file-reader source.
	SourceFile = "source.file"
	// SourceHTTPMethod and SourceHTTPPath describe the inbound request on
	// an http-listener source.
	SourceHTTPMethod = "source.http.method"
	SourceHTTPPath   = "source.http.path"
	// SourceHTTPContentType is the inbound request's Content-Type.
	SourceHTTPContentType = "source.http.header.content-type"
	// SourceHTTPQueryPrefix prefixes one key per inbound query parameter:
	// source.http.query.<name> = first value.
	SourceHTTPQueryPrefix = "source.http.query."
)

// Outbound routing hints (script-set, read by http-sender).
const (
	HTTPMethod = "http.method"
	HTTPPath   = "http.path"
)
