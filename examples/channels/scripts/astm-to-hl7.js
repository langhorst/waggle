// Message Translator: ASTM E1394 result message → HL7 v2 ORU^R01.
// H → MSH, P → PID, O → OBR, each R → one OBX. Components are copied
// field-by-field so separators never need re-escaping.
function transform(msg) {
	var out = newMessage('hl7v2');
	out.set('MSH-1', '|');
	out.set('MSH-2', '^~\\&');
	out.set('MSH-3', msg.get('H-5') || 'ASTM');
	out.set('MSH-7', msg.get('H-14') || '');
	out.set('MSH-9.1', 'ORU');
	out.set('MSH-9.2', 'R01');
	out.set('MSH-10', 'ASTM' + (msg.get('H-14') || '0'));
	out.set('MSH-11', 'P');
	out.set('MSH-12', '2.5.1');

	out.set('PID-1', '1');
	out.set('PID-3.1', msg.get('P-3.1') || '');
	out.set('PID-5.1', msg.get('P-5.1') || '');
	out.set('PID-5.2', msg.get('P-5.2') || '');
	out.set('PID-7', msg.get('P-7') || '');
	out.set('PID-8', msg.get('P-8') || '');

	out.set('OBR-1', '1');
	out.set('OBR-2', msg.get('O-2') || '');
	out.set('OBR-4.1', msg.get('O-4.4') || ''); // ASTM universal test ID lives in component 4
	out.set('OBR-7', msg.get('O-6') || '');

	var results = msg.segments('R');
	for (var i = 0; i < results.length; i++) {
		var n = i + 1;
		var prefix = 'OBX[' + n + ']';
		out.set(prefix + '-1', String(n));
		out.set(prefix + '-2', 'TX');
		out.set(prefix + '-3.1', results[i].get('2.4') || '');
		out.set(prefix + '-5', results[i].get('3') || '');
		out.set(prefix + '-6', results[i].get('4') || '');
		out.set(prefix + '-8', results[i].get('6') || '');
		out.set(prefix + '-11', results[i].get('8') || 'F');
	}
	return out;
}
