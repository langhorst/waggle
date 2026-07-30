// Message Translator: HL7 v2 ORM^O01 order → ASTM E1394 order message.
// MSH → H, PID → P, each ORC/OBR pair → one O record (order download to an
// instrument), closed by an L record. ORC-1 order control maps onto the
// ASTM O-11 action code: NW (new) → N, CA (cancel) → C, anything else → A
// (add to existing specimen).
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

	var orders = msg.segments('OBR');
	var controls = msg.segments('ORC');
	for (var i = 0; i < orders.length; i++) {
		var n = i + 1;
		var prefix = 'O[' + n + ']';
		out.set(prefix + '-1', String(n));
		out.set(prefix + '-2', orders[i].get('2') || '');       // placer/specimen ID
		out.set(prefix + '-4.4', orders[i].get('4.1') || '');   // universal test ID
		out.set(prefix + '-5', orders[i].get('5') || 'R');      // priority
		out.set(prefix + '-6', orders[i].get('7') || '');       // requested date/time
		var control = i < controls.length ? controls[i].get('1') : '';
		var action = 'A';
		if (control === 'NW') { action = 'N'; }
		else if (control === 'CA') { action = 'C'; }
		out.set(prefix + '-11', action);
	}

	out.set('L-1', '1');
	out.set('L-2', 'N');
	return out;
}
