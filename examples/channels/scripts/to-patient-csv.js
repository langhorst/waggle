// One CSV row per ADT message: id, last name, first name, date of birth.
//
// A row per message rather than per patient, deliberately: the file is a
// log of what actually arrived, so an A08 that corrects a name shows up as
// a second row with the new value. `sort -u` collapses it to one row per
// patient when that is what you want.
function transform(msg) {
  var out = newMessage('csv');
  out.set('R.1', msg.get('PID-3[1].1'));  // MRN, first identifier repetition
  out.set('R.2', msg.get('PID-5.1'));     // family name
  out.set('R.3', msg.get('PID-5.2'));     // given name
  out.set('R.4', msg.get('PID-7'));       // date of birth, YYYYMMDD
  return out;
}
