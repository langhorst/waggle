// Message Translator: FHIR R4 Patient resource (XML) → HL7 v2 ADT^A31.
// The XML sibling of fhir-patient-to-adt.js: FHIR XML carries primitives
// in value attributes, so paths read Patient/id/@value. The default xmlns
// on <Patient> is transparent — child elements are unprefixed as written.
// Non-Patient documents reject with AR (HTTP 400 in destination ACK mode).
function transform(msg) {
	if (!msg.get('Patient')) {
		response.reject('AR', 'expected a Patient resource');
	}
	var out = newMessage('hl7v2');
	out.set('MSH-1', '|');
	out.set('MSH-2', '^~\\&');
	out.set('MSH-3', 'FHIR-XML');
	out.set('MSH-9.1', 'ADT');
	out.set('MSH-9.2', 'A31');
	out.set('MSH-10', 'FHIRX' + (msg.get('Patient/id/@value') || '0'));
	out.set('MSH-11', 'P');
	out.set('MSH-12', '2.5.1');

	out.set('PID-1', '1');
	out.set('PID-3.1', msg.get('Patient/identifier/value/@value') || msg.get('Patient/id/@value') || '');
	out.set('PID-5.1', msg.get('Patient/name[1]/family/@value') || '');
	out.set('PID-5.2', msg.get('Patient/name[1]/given[1]/@value') || '');
	var dob = msg.get('Patient/birthDate/@value') || '';
	out.set('PID-7', dob.split('-').join('')); // FHIR YYYY-MM-DD → HL7 YYYYMMDD
	var gender = msg.get('Patient/gender/@value');
	out.set('PID-8', gender === 'male' ? 'M' : gender === 'female' ? 'F' : 'U');
	return out;
}
