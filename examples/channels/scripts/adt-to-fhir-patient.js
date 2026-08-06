// Message Translator: HL7 v2 ADT → FHIR R4-shaped Patient resource (JSON).
// Values keep their JSON types end to end: active stays a boolean on the
// wire because msg.set passes native JS types through to the json format.
// A28/A31-style updates PUT to the patient's own URL via the meta routing
// keys; everything else POSTs to the configured collection URL.
function transform(msg) {
	var mrn = msg.get('PID-3.1') || '';
	var out = newMessage('json');
	out.set('resourceType', 'Patient');
	out.set('id', mrn);
	out.set('identifier[0].system', 'urn:waggle:mrn');
	out.set('identifier[0].value', mrn);
	out.set('active', true);
	out.set('name[0].family', msg.get('PID-5.1') || '');
	if (msg.get('PID-5.2')) {
		out.set('name[0].given[0]', msg.get('PID-5.2'));
	}
	var dob = msg.get('PID-7') || '';
	if (dob.length >= 8) { // HL7 YYYYMMDD → FHIR YYYY-MM-DD
		out.set('birthDate', dob.substr(0, 4) + '-' + dob.substr(4, 2) + '-' + dob.substr(6, 2));
	}
	var sex = msg.get('PID-8');
	out.set('gender', sex === 'M' ? 'male' : sex === 'F' ? 'female' : 'unknown');

	var trigger = msg.get('MSH-9.2');
	if ((trigger === 'A28' || trigger === 'A31') && mrn) {
		meta['http.method'] = 'PUT';
		meta['http.path'] = '/fhir/Patient/' + mrn;
	}
	return out;
}
