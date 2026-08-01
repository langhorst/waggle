// Message Filter: only ADT events pass; everything else is FILTERED
// (retained and replayable, but never delivered).
function filter(msg) {
	return msg.get('MSH-9.1') === 'ADT';
}
