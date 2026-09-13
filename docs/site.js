/* RedisNearCache docs: syntax highlighting, copy buttons, active-section nav.
   Everything here is progressive enhancement. With JavaScript off the page is
   still complete: code is plain text, the nav still links to every anchor. */
(function () {
  'use strict';

  /* ---------------- syntax highlighting ---------------- */

  var CSHARP_KEYWORDS = [
    'abstract', 'as', 'async', 'await', 'base', 'bool', 'break', 'byte', 'case', 'catch', 'char',
    'class', 'const', 'continue', 'default', 'delegate', 'do', 'double', 'else', 'enum', 'event',
    'explicit', 'extern', 'false', 'finally', 'fixed', 'float', 'for', 'foreach', 'get', 'global',
    'goto', 'if', 'implicit', 'in', 'init', 'int', 'interface', 'internal', 'is', 'lock', 'long',
    'namespace', 'new', 'null', 'object', 'operator', 'out', 'override', 'params', 'private',
    'protected', 'public', 'readonly', 'record', 'ref', 'return', 'sealed', 'set', 'short',
    'sizeof', 'stackalloc', 'static', 'string', 'struct', 'switch', 'this', 'throw', 'true', 'try',
    'typeof', 'uint', 'ulong', 'unsafe', 'ushort', 'using', 'var', 'virtual', 'void', 'while', 'yield'
  ];

  var RULES = {
    csharp: [
      { re: /\/\/[^\n]*/y, cls: 'tok-comment' },
      { re: /@?"(?:[^"\\\n]|\\.)*"/y, cls: 'tok-string' },
      { re: /'(?:[^'\\\n]|\\.)*'/y, cls: 'tok-string' },
      { re: new RegExp('\\b(?:' + CSHARP_KEYWORDS.join('|') + ')\\b', 'y'), cls: 'tok-keyword' },
      { re: /\b[A-Z][A-Za-z0-9_]*\b/y, cls: 'tok-type' },
      { re: /\b\d[\d_]*(?:\.\d+)?\b/y, cls: 'tok-number' }
    ],
    bash: [
      { re: /#[^\n]*/y, cls: 'tok-comment' },
      { re: /"(?:[^"\\\n]|\\.)*"/y, cls: 'tok-string' },
      { re: /'[^'\n]*'/y, cls: 'tok-string' },
      { re: /\b(?:dotnet|docker|curl|redis-cli|python3|pkill|export)\b/y, cls: 'tok-keyword' },
      { re: /--?[A-Za-z][\w-]*/y, cls: 'tok-type' },
      { re: /\b\d[\d_.]*\b/y, cls: 'tok-number' }
    ],
    json: [
      { re: /"(?:[^"\\\n]|\\.)*"(?=\s*:)/y, cls: 'tok-type' },
      { re: /"(?:[^"\\\n]|\\.)*"/y, cls: 'tok-string' },
      { re: /\b(?:true|false|null)\b/y, cls: 'tok-keyword' },
      { re: /-?\b\d+(?:\.\d+)?\b/y, cls: 'tok-number' }
    ]
  };
  RULES.shell = RULES.bash;

  function escapeHtml(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  function highlight(text, rules) {
    var out = '';
    var plain = '';
    var i = 0;
    while (i < text.length) {
      var matched = false;
      for (var r = 0; r < rules.length; r++) {
        var rule = rules[r];
        rule.re.lastIndex = i;
        var m = rule.re.exec(text);
        if (m && m[0].length > 0) {
          if (plain) { out += escapeHtml(plain); plain = ''; }
          out += '<span class="' + rule.cls + '">' + escapeHtml(m[0]) + '</span>';
          i += m[0].length;
          matched = true;
          break;
        }
      }
      if (!matched) { plain += text[i]; i++; }
    }
    if (plain) { out += escapeHtml(plain); }
    return out;
  }

  function languageOf(codeEl) {
    var cls = codeEl.className || '';
    var m = cls.match(/language-([\w-]+)/);
    return m ? m[1] : 'text';
  }

  var codes = document.querySelectorAll('.codeblock pre > code');
  for (var c = 0; c < codes.length; c++) {
    var lang = languageOf(codes[c]);
    var rules = RULES[lang];
    if (!rules) { continue; }
    try {
      codes[c].innerHTML = highlight(codes[c].textContent, rules);
    } catch (err) {
      /* leave the plain text alone */
    }
  }

  /* ---------------- copy buttons ---------------- */

  var blocks = document.querySelectorAll('.codeblock');
  for (var b = 0; b < blocks.length; b++) {
    (function (block) {
      var head = block.querySelector('.cb-head');
      var pre = block.querySelector('pre');
      if (!head || !pre) { return; }
      var button = document.createElement('button');
      button.type = 'button';
      button.className = 'copy';
      button.textContent = 'Copy';
      button.setAttribute('aria-label', 'Copy code to clipboard');
      button.addEventListener('click', function () {
        var text = pre.textContent;
        var done = function () {
          button.textContent = 'Copied';
          window.setTimeout(function () { button.textContent = 'Copy'; }, 1400);
        };
        if (navigator.clipboard && navigator.clipboard.writeText) {
          navigator.clipboard.writeText(text).then(done, function () { button.textContent = 'Press Ctrl+C'; });
        } else {
          var ta = document.createElement('textarea');
          ta.value = text;
          document.body.appendChild(ta);
          ta.select();
          try { document.execCommand('copy'); done(); } catch (e) { button.textContent = 'Press Ctrl+C'; }
          document.body.removeChild(ta);
        }
      });
      head.appendChild(button);
    })(blocks[b]);
  }

  /* ---------------- active section in the nav ---------------- */

  var links = {};
  var navAnchors = document.querySelectorAll('.sidebar a[href^="#"]');
  for (var n = 0; n < navAnchors.length; n++) {
    links[navAnchors[n].getAttribute('href').slice(1)] = navAnchors[n];
  }

  function setActive(id) {
    for (var key in links) {
      if (Object.prototype.hasOwnProperty.call(links, key)) {
        links[key].classList.toggle('active', key === id);
      }
    }
  }

  var targets = document.querySelectorAll('.section, .member');
  if ('IntersectionObserver' in window && targets.length) {
    var visible = {};
    var observer = new IntersectionObserver(function (entries) {
      for (var e = 0; e < entries.length; e++) {
        var id = entries[e].target.id;
        if (entries[e].isIntersecting) { visible[id] = entries[e].boundingClientRect.top; }
        else { delete visible[id]; }
      }
      var best = null;
      var bestTop = Infinity;
      for (var key in visible) {
        if (!Object.prototype.hasOwnProperty.call(visible, key)) { continue; }
        if (!links[key]) { continue; }
        var top = document.getElementById(key).getBoundingClientRect().top;
        if (top < bestTop) { bestTop = top; best = key; }
      }
      if (best) { setActive(best); }
    }, { rootMargin: '-60px 0px -55% 0px', threshold: 0 });

    for (var t = 0; t < targets.length; t++) { observer.observe(targets[t]); }
  }

  if (window.location.hash && links[window.location.hash.slice(1)]) {
    setActive(window.location.hash.slice(1));
  }
})();
