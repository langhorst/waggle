// Keep only ADT messages that carry patient demographics.
//
// Every trigger this channel sees (A01, A02, A03, A08, A11) has a PID, but
// a feed can also carry acknowledgements and queries that do not. Dropping
// them here keeps the CSV and the FHIR server free of blank rows and
// resources with no identifier.
function filter(msg) {
  var mrn = msg.get('PID-3[1].1');
  return mrn !== '' && mrn !== null && mrn !== undefined;
}
