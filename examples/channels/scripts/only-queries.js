// Message Filter: pass only ASTM query messages (those carrying a Q
// record). Anything else — results, orders, comments — is FILTERED:
// retained and replayable, but never delivered to the query responder.
function filter(msg) {
	return msg.segments('Q').length > 0;
}
