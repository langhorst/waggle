// Message Translator step: normalize the family name.
function transform(msg) {
	var name = msg.get('PID-5.1');
	if (name) { msg.set('PID-5.1', name.toUpperCase()); }
}
