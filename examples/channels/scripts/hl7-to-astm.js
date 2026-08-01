// Message Translator: HL7 v2 ORU^R01 → ASTM E1394 result message.
// MSH → H, PID → P, OBR → O, each OBX → one R, closed by an L record.
// The inverse of astm-to-hl7.js: fields map component-by-component.
function transform(msg) {
	var out = newMessage('astm');
	out.set('H-1', '|');
	out.set('H-2', '\\^&');
	out.set('H-5', msg.get('MSH-3') || 'LIS');
	out.set('H-12', 'P');
	out.set('H-13', 'LIS2-A2');
	out.set('H-14', msg.get('MSH-7') || '');

	out.set('P-1', '1');
	out.set('P-3.1', msg.get('PID-3.1') || '');
	out.set('P-5.1', msg.get('PID-5.1') || '');
	out.set('P-5.2', msg.get('PID-5.2') || '');
	out.set('P-7', msg.get('PID-7') || '');
	out.set('P-8', msg.get('PID-8') || '');

	out.set('O-1', '1');
	out.set('O-2', msg.get('OBR-2') || '');
	out.set('O-4.4', msg.get('OBR-4.1') || '');
	out.set('O-6', msg.get('OBR-7') || '');

	var observations = msg.segments('OBX');
	for (var i = 0; i < observations.length; i++) {
		var n = i + 1;
		var prefix = 'R[' + n + ']';
		out.set(prefix + '-1', String(n));
		out.set(prefix + '-2.4', observations[i].get('3.1') || '');
		out.set(prefix + '-3', observations[i].get('5') || '');
		out.set(prefix + '-4', observations[i].get('6') || '');
		out.set(prefix + '-6', observations[i].get('8') || '');
		out.set(prefix + '-8', observations[i].get('11') || 'F');
	}

	out.set('L-1', '1');
	out.set('L-2', 'N');
	return out;
}
