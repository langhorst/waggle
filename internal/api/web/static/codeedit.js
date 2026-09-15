// A small JavaScript syntax highlighter and editor, with no dependencies:
// the web UI is embedded in the binary and must work offline, so pulling a
// highlighter from a CDN is not an option.
//
// The editor is a transparent <textarea> stacked over a <pre> holding the
// highlighted copy. The textarea keeps every native behaviour (caret,
// selection, undo, IME, spellcheck-off) and the layer beneath supplies the
// colour; the two are kept aligned by sharing their font metrics in CSS and
// mirroring scroll position here.
(function () {
  'use strict';

  const KEYWORDS = new Set([
    'async', 'await', 'break', 'case', 'catch', 'class', 'const', 'continue',
    'debugger', 'default', 'delete', 'do', 'else', 'export', 'extends',
    'finally', 'for', 'from', 'function', 'get', 'if', 'import', 'in',
    'instanceof', 'let', 'new', 'of', 'return', 'set', 'static', 'super',
    'switch', 'this', 'throw', 'try', 'typeof', 'var', 'void', 'while',
    'with', 'yield',
  ]);

  const LITERALS = new Set(['true', 'false', 'null', 'undefined', 'NaN', 'Infinity']);

  // Globals the goja runtime injects, plus the standard objects a transform
  // is likely to reach for.
  const BUILTINS = new Set([
    'msg', 'meta', 'logger', 'response', 'newMessage',
    'console', 'JSON', 'Math', 'Date', 'Object', 'Array', 'String', 'Number',
    'Boolean', 'RegExp', 'Error', 'Map', 'Set', 'Promise', 'Symbol',
    'parseInt', 'parseFloat', 'isNaN', 'isFinite', 'encodeURIComponent',
    'decodeURIComponent',
  ]);

  // A '/' opens a regex literal only where a value may begin. After a value
  // -- an identifier, number, string, ')' or ']' -- it is division instead.
  const VALUE_KEYWORDS = new Set([
    'return', 'typeof', 'instanceof', 'in', 'of', 'new', 'delete', 'void',
    'case', 'do', 'else', 'yield', 'await',
  ]);

  function regexAllowed(prev) {
    if (!prev) return true;
    if (prev.type === 'name') return VALUE_KEYWORDS.has(prev.text);
    if (prev.type === 'number' || prev.type === 'string' || prev.type === 'regex') return false;
    if (prev.type === 'punct') return !(prev.text === ')' || prev.text === ']');
    return true;
  }

  const isSpace = (c) => c === ' ' || c === '\t' || c === '\n' || c === '\r' || c === '\f' || c === '\v';
  const isDigit = (c) => c >= '0' && c <= '9';
  const isIdentStart = (c) => (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c === '_' || c === '$';
  const isIdent = (c) => isIdentStart(c) || isDigit(c);

  // tokenize returns a flat list of {type, text} covering src exactly, so
  // joining the texts reproduces the input.
  function tokenize(src) {
    const out = [];
    let i = 0;
    let prev = null; // last token that is not whitespace or a comment
    const push = (type, text) => {
      out.push({ type: type, text: text });
      if (type !== 'ws' && type !== 'comment') prev = out[out.length - 1];
    };

    while (i < src.length) {
      const c = src[i];

      if (isSpace(c)) {
        let j = i + 1;
        while (j < src.length && isSpace(src[j])) j++;
        push('ws', src.slice(i, j));
        i = j;
        continue;
      }

      // Comments.
      if (c === '/' && src[i + 1] === '/') {
        let j = i + 2;
        while (j < src.length && src[j] !== '\n') j++;
        push('comment', src.slice(i, j));
        i = j;
        continue;
      }
      if (c === '/' && src[i + 1] === '*') {
        const end = src.indexOf('*/', i + 2);
        const j = end === -1 ? src.length : end + 2;
        push('comment', src.slice(i, j));
        i = j;
        continue;
      }

      // Regex literal, where the grammar allows one to start.
      if (c === '/' && regexAllowed(prev)) {
        const j = scanRegex(src, i);
        if (j > i) {
          push('regex', src.slice(i, j));
          i = j;
          continue;
        }
      }

      // Single and double quoted strings.
      if (c === '"' || c === "'") {
        const j = scanQuoted(src, i, c);
        push('string', src.slice(i, j));
        i = j;
        continue;
      }

      // Template literals: the quoted parts stay strings while each ${...}
      // is tokenized as the code it is.
      if (c === '`') {
        i = scanTemplate(src, i, push);
        continue;
      }

      if (isDigit(c) || (c === '.' && isDigit(src[i + 1]))) {
        const j = scanNumber(src, i);
        push('number', src.slice(i, j));
        i = j;
        continue;
      }

      if (isIdentStart(c)) {
        let j = i + 1;
        while (j < src.length && isIdent(src[j])) j++;
        const word = src.slice(i, j);
        push('name', word);
        i = j;
        continue;
      }

      push('punct', c);
      i++;
    }
    return out;
  }

  function scanQuoted(src, start, quote) {
    let i = start + 1;
    while (i < src.length) {
      const c = src[i];
      if (c === '\\') { i += 2; continue; }
      if (c === quote) return i + 1;
      if (c === '\n') return i; // unterminated: stop at the line end
      i++;
    }
    return i;
  }

  // scanTemplate emits the literal chunks as strings and recurses into each
  // interpolation. It returns the index just past the closing backtick.
  function scanTemplate(src, start, push) {
    let i = start + 1;
    let chunk = '`';
    while (i < src.length) {
      const c = src[i];
      if (c === '\\') { chunk += src.slice(i, i + 2); i += 2; continue; }
      if (c === '`') {
        push('string', chunk + '`');
        return i + 1;
      }
      if (c === '$' && src[i + 1] === '{') {
        push('string', chunk);
        chunk = '';
        const end = matchBrace(src, i + 1);
        push('punct', '${');
        for (const t of tokenize(src.slice(i + 2, end))) push(t.type, t.text);
        if (end < src.length) push('punct', '}');
        i = end + 1;
        continue;
      }
      chunk += c;
      i++;
    }
    if (chunk) push('string', chunk);
    return i;
  }

  // matchBrace returns the index of the '}' closing the '{' at open, or the
  // source length when it is unbalanced.
  function matchBrace(src, open) {
    let depth = 0;
    for (let i = open; i < src.length; i++) {
      const c = src[i];
      if (c === '{') depth++;
      else if (c === '}') { depth--; if (depth === 0) return i; }
      else if (c === '"' || c === "'") i = scanQuoted(src, i, c) - 1;
      else if (c === '`') { const j = scanTemplateEnd(src, i); i = j - 1; }
    }
    return src.length;
  }

  // scanTemplateEnd finds the end of a template literal without emitting
  // tokens, for brace matching.
  function scanTemplateEnd(src, start) {
    let i = start + 1;
    while (i < src.length) {
      const c = src[i];
      if (c === '\\') { i += 2; continue; }
      if (c === '`') return i + 1;
      if (c === '$' && src[i + 1] === '{') { i = matchBrace(src, i + 1) + 1; continue; }
      i++;
    }
    return i;
  }

  function scanNumber(src, start) {
    let i = start;
    if (src[i] === '0' && /[xXbBoO]/.test(src[i + 1] || '')) {
      i += 2;
      while (i < src.length && /[0-9a-fA-F_]/.test(src[i])) i++;
    } else {
      while (i < src.length && /[0-9_]/.test(src[i])) i++;
      if (src[i] === '.') { i++; while (i < src.length && /[0-9_]/.test(src[i])) i++; }
      if (/[eE]/.test(src[i] || '')) {
        let j = i + 1;
        if (src[j] === '+' || src[j] === '-') j++;
        if (isDigit(src[j])) { i = j; while (i < src.length && isDigit(src[i])) i++; }
      }
    }
    if (src[i] === 'n') i++; // BigInt
    return i;
  }

  // scanRegex returns the index past the closing '/' and flags, or start
  // when this '/' does not in fact open a well-formed literal.
  function scanRegex(src, start) {
    let i = start + 1;
    let inClass = false;
    while (i < src.length) {
      const c = src[i];
      if (c === '\\') { i += 2; continue; }
      if (c === '\n') return start;
      if (inClass) {
        if (c === ']') inClass = false;
      } else if (c === '[') {
        inClass = true;
      } else if (c === '/') {
        i++;
        while (i < src.length && /[a-z]/.test(src[i])) i++;
        return i;
      }
      i++;
    }
    return start;
  }

  // classify maps a bare identifier to its token class using the tokens
  // either side of it: `.foo` is a property, `foo(` a call.
  function classify(tokens, index) {
    const text = tokens[index].text;
    if (KEYWORDS.has(text)) return 'keyword';
    if (LITERALS.has(text)) return 'literal';

    let before = null;
    for (let i = index - 1; i >= 0; i--) {
      if (tokens[i].type !== 'ws' && tokens[i].type !== 'comment') { before = tokens[i]; break; }
    }
    let after = null;
    for (let i = index + 1; i < tokens.length; i++) {
      if (tokens[i].type !== 'ws' && tokens[i].type !== 'comment') { after = tokens[i]; break; }
    }
    if (before && before.type === 'punct' && before.text === '.') {
      return after && after.text === '(' ? 'function' : 'property';
    }
    if (after && after.text === '(') return 'function';
    if (BUILTINS.has(text)) return 'builtin';
    return '';
  }

  const ESCAPES = { '&': '&amp;', '<': '&lt;', '>': '&gt;' };
  const escapeHTML = (s) => s.replace(/[&<>]/g, (ch) => ESCAPES[ch]);

  // highlight renders src as HTML. Script bodies are operator-supplied, so
  // every character is escaped before it reaches innerHTML.
  function highlight(src) {
    const tokens = tokenize(src);
    let html = '';
    for (let i = 0; i < tokens.length; i++) {
      const t = tokens[i];
      const text = escapeHTML(t.text);
      let cls = '';
      switch (t.type) {
        case 'ws': html += text; continue;
        case 'name': cls = classify(tokens, i); break;
        case 'comment': cls = 'comment'; break;
        case 'string': cls = 'string'; break;
        case 'number': cls = 'number'; break;
        case 'regex': cls = 'regex'; break;
        case 'punct': cls = 'punct'; break;
      }
      html += cls ? '<span class="tok-' + cls + '">' + text + '</span>' : text;
    }
    return html;
  }

  // attach wires a textarea into the highlighted editor. The textarea is
  // moved inside a generated wrapper, so markup keeps a plain <textarea>
  // that still works when this script does not run.
  function attach(textarea, opts) {
    const options = opts || {};
    const wrapper = document.createElement('div');
    wrapper.className = 'code-editor';
    if (options.height) wrapper.style.setProperty('--ce-height', options.height);

    const scroll = document.createElement('div');
    scroll.className = 'code-editor__scroll';

    const gutter = document.createElement('div');
    gutter.className = 'code-editor__gutter';
    gutter.setAttribute('aria-hidden', 'true');
    const gutterInner = document.createElement('div');
    gutterInner.className = 'code-editor__gutter-inner';
    gutter.appendChild(gutterInner);

    const pane = document.createElement('div');
    pane.className = 'code-editor__pane';
    const pre = document.createElement('pre');
    pre.className = 'code-editor__highlight';
    pre.setAttribute('aria-hidden', 'true');
    const code = document.createElement('code');
    pre.appendChild(code);

    textarea.parentNode.insertBefore(wrapper, textarea);
    pane.appendChild(pre);
    pane.appendChild(textarea);
    scroll.appendChild(gutter);
    scroll.appendChild(pane);
    wrapper.appendChild(scroll);

    textarea.classList.remove('code-editor-fallback');
    textarea.classList.add('code-editor__input');
    textarea.setAttribute('wrap', 'off');
    textarea.setAttribute('spellcheck', 'false');
    textarea.setAttribute('autocapitalize', 'off');
    textarea.setAttribute('autocomplete', 'off');

    let lastLines = -1;

    function renderGutter(text) {
      // A trailing newline opens a further line in the textarea.
      const lines = text.split('\n').length;
      if (lines === lastLines) return;
      lastLines = lines;
      let s = '';
      for (let n = 1; n <= lines; n++) s += n + '\n';
      gutterInner.textContent = s;
    }

    function render() {
      const text = textarea.value;
      // A trailing newline would otherwise collapse in the <pre>, leaving
      // the layer one line shorter than the textarea.
      code.innerHTML = highlight(text) + (text.endsWith('\n') ? '\n' : '');
      renderGutter(text);
      syncScroll();
    }

    function syncScroll() {
      pre.scrollTop = textarea.scrollTop;
      pre.scrollLeft = textarea.scrollLeft;
      gutter.scrollTop = textarea.scrollTop;
    }

    textarea.addEventListener('input', render);
    textarea.addEventListener('scroll', syncScroll);

    // Tab indents instead of leaving the field: this is an editor, and the
    // surrounding page has no other tab stop worth reaching mid-edit.
    textarea.addEventListener('keydown', (e) => {
      if (e.key !== 'Tab' || e.ctrlKey || e.metaKey || e.altKey) return;
      e.preventDefault();
      const start = textarea.selectionStart;
      const end = textarea.selectionEnd;
      const value = textarea.value;
      if (e.shiftKey) {
        // Outdent: drop up to two spaces before the caret.
        const lineStart = value.lastIndexOf('\n', start - 1) + 1;
        const indent = value.slice(lineStart, start).match(/[ ]{1,2}$/);
        if (!indent) return;
        textarea.value = value.slice(0, start - indent[0].length) + value.slice(start);
        textarea.selectionStart = textarea.selectionEnd = start - indent[0].length;
      } else {
        textarea.value = value.slice(0, start) + '  ' + value.slice(end);
        textarea.selectionStart = textarea.selectionEnd = start + 2;
      }
      render();
    });

    render();
    return { render: render };
  }

  window.CodeEdit = { attach: attach, highlight: highlight, tokenize: tokenize };
})();
