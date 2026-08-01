// Message Translator: ASTM query (Q record) → ASTM order message answering
// it. An instrument that reads a sample it has no order for asks the LIS
// "what should I run on specimen X?" (Q-2 starting range = ^specimenID);
// the response is an order message whose O-11 action code Q marks it as a
// response to query.
//
// This demo derives the order from the query itself (echoing the specimen
// and requested test, defaulting to ALL). A real site would look the order
// up — swap the body of this script for that lookup; the channel shape
// (astm-listener in, only-queries.js filter, astm-sender back out) stays
// the same.
function transform(msg) {
	var specimen = msg.get('Q-2.2') || msg.get('Q-2') || '';
	var test = msg.get('Q-3.4') || '';

	var out = newMessage('astm');
	out.set('H-1', '|');
	out.set('H-2', '\\^&');
	out.set('H-5', 'LIS');
	out.set('H-12', 'P');
	out.set('H-13', 'LIS2-A2');
	out.set('H-14', msg.get('H-14') || '');

	out.set('O-1', '1');
	out.set('O-2', specimen);
	out.set('O-4.4', test || 'ALL');
	out.set('O-5', 'R');
	out.set('O-11', 'Q'); // response to query

	out.set('L-1', '1');
	out.set('L-2', 'F'); // final: no more data for this query
	return out;
}
