// Message Translator with format conversion: build a CSV row from the HL7
// tree. Returning a newMessage() replaces the outgoing tree and data type.
function transform(msg) {
	var out = newMessage('csv');
	out.set('R.1', msg.get('PID-3.1'));   // MRN
	out.set('R.2', msg.get('PID-5.1'));   // family name
	out.set('R.3', msg.get('PID-5.2'));   // given name
	out.set('R.4', msg.get('PID-7'));     // date of birth
	return out;
}
