// Message Translator: FHIR R4 Patient resource (JSON) → HL7 v2 ADT^A31
// (update person information). Rejects non-Patient payloads with AR — in the
// webhook channel's destination ACK mode the caller sees that as HTTP 400.
function transform(msg) {
	if (msg.get('resourceType') !== 'Patient') {
		response.reject('AR', 'expected a Patient resource, got ' + (msg.get('resourceType') || 'nothing'));
	}
	var out = newMessage('hl7v2');
	out.set('MSH-1', '|');
	out.set('MSH-2', '^~\\&');
	out.set('MSH-3', 'FHIR');
	out.set('MSH-9.1', 'ADT');
	out.set('MSH-9.2', 'A31');
	out.set('MSH-10', 'FHIR' + (msg.get('id') || '0'));
	out.set('MSH-11', 'P');
	out.set('MSH-12', '2.5.1');

	out.set('PID-1', '1');
	out.set('PID-3.1', msg.get('identifier[0].value') || msg.get('id') || '');
	out.set('PID-5.1', msg.get('name[0].family') || '');
	out.set('PID-5.2', msg.get('name[0].given[0]') || '');
	var dob = msg.get('birthDate') || '';
	out.set('PID-7', dob.split('-').join('')); // FHIR YYYY-MM-DD → HL7 YYYYMMDD
	var gender = msg.get('gender');
	out.set('PID-8', gender === 'male' ? 'M' : gender === 'female' ? 'F' : 'U');
	return out;
}
