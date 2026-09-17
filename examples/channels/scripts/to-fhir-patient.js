// Message Translator: HL7 v2 ADT -> FHIR R4 Patient.
//
// Every ADT message for a patient writes the SAME resource, by addressing it
// with the MRN: PUT [base]/Patient/<mrn>. An A01 creates it, the A08 that
// corrects a name updates it in place, and the A02s and A03s in between are
// harmless rewrites of the same demographics. POSTing instead would create a
// fresh Patient per message and leave dozens of duplicates per person, which
// makes the server impossible to eyeball.
//
// This relies on "update as create" (FHIR R4 §3.1.0.7), which nearly every
// server supports. If yours refuses client-assigned ids, switch to a
// conditional update by swapping the two lines at the bottom: it is the same
// idempotency by a different route, and lands one Patient per MRN either way.
//
// The path is relative, so the FHIR root lives only in the channel's
// adapter url (which must end in a slash for the two to join correctly).
function transform(msg) {
  var mrn = msg.get('PID-3[1].1') || '';

  var out = newMessage('json');
  out.set('resourceType', 'Patient');
  out.set('id', mrn);

  // The MRN, qualified by the facility that assigned it (PID-3.4), plus the
  // enterprise id from the second repetition. Sending both is what lets a
  // query find the patient by either.
  out.set('identifier[0].use', 'usual');
  out.set('identifier[0].type.coding[0].system', 'http://terminology.hl7.org/CodeSystem/v2-0203');
  out.set('identifier[0].type.coding[0].code', 'MR');
  out.set('identifier[0].system', 'urn:waggle:mrn:' + (msg.get('PID-3[1].4') || 'unknown'));
  out.set('identifier[0].value', mrn);

  var enterprise = msg.get('PID-3[2].1');
  if (enterprise) {
    out.set('identifier[1].use', 'secondary');
    out.set('identifier[1].system', 'urn:waggle:enterprise-id');
    out.set('identifier[1].value', enterprise);
  }

  out.set('active', true);

  out.set('name[0].use', 'official');
  out.set('name[0].family', msg.get('PID-5.1') || '');
  if (msg.get('PID-5.2')) {
    out.set('name[0].given[0]', msg.get('PID-5.2'));
  }
  if (msg.get('PID-5.3')) {
    out.set('name[0].given[1]', msg.get('PID-5.3'));
  }

  // HL7 YYYYMMDD -> FHIR date YYYY-MM-DD.
  var dob = msg.get('PID-7') || '';
  if (dob.length >= 8) {
    out.set('birthDate', dob.substr(0, 4) + '-' + dob.substr(4, 2) + '-' + dob.substr(6, 2));
  }

  // PID-8 (HL7 table 0001) -> FHIR administrative-gender.
  var sex = msg.get('PID-8');
  out.set('gender', sex === 'M' ? 'male' : sex === 'F' ? 'female' : sex === 'O' ? 'other' : 'unknown');

  var phone = msg.get('PID-13');
  if (phone) {
    out.set('telecom[0].system', 'phone');
    out.set('telecom[0].value', phone);
    out.set('telecom[0].use', 'home');
  }

  // PID-11: street^other^city^state^zip^country.
  if (msg.get('PID-11.1')) {
    out.set('address[0].use', 'home');
    out.set('address[0].line[0]', msg.get('PID-11.1'));
    out.set('address[0].city', msg.get('PID-11.3') || '');
    out.set('address[0].state', msg.get('PID-11.4') || '');
    out.set('address[0].postalCode', msg.get('PID-11.5') || '');
    if (msg.get('PID-11.6')) {
      out.set('address[0].country', msg.get('PID-11.6'));
    }
  }

  meta['http.method'] = 'PUT';
  meta['http.path'] = 'Patient/' + mrn;

  // Conditional-update alternative, for a server that will not take a
  // client-assigned id. Delete the `out.set('id', ...)` line above as well:
  //   meta['http.path'] = 'Patient?identifier=urn:waggle:mrn:'
  //     + (msg.get('PID-3[1].4') || 'unknown') + '|' + mrn;

  return out;
}
