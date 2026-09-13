/* 8-битные звуки на голых осцилляторах — без файлов и задержек. */
(function () {
  "use strict";

  var ctx = null;
  var enabled = true;
  var STORAGE_KEY = "obshak.sound";

  try {
    var stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored === "off") { enabled = false; }
  } catch (error) { /* приватный режим — просто без сохранения */ }

  function audio() {
    if (!enabled) { return null; }
    if (ctx) { return ctx; }
    var Ctor = window.AudioContext || window.webkitAudioContext;
    if (!Ctor) { return null; }
    try { ctx = new Ctor(); } catch (error) { ctx = null; }
    return ctx;
  }

  function tone(freq, duration, options) {
    var context = audio();
    if (!context) { return; }
    options = options || {};
    if (context.state === "suspended") { context.resume(); }
    var start = context.currentTime + (options.delay || 0);
    var osc = context.createOscillator();
    var gain = context.createGain();
    osc.type = options.type || "square";
    osc.frequency.setValueAtTime(freq, start);
    if (options.slideTo) {
      osc.frequency.exponentialRampToValueAtTime(Math.max(30, options.slideTo), start + duration);
    }
    var peak = options.gain || 0.06;
    gain.gain.setValueAtTime(0.0001, start);
    gain.gain.exponentialRampToValueAtTime(peak, start + 0.008);
    gain.gain.exponentialRampToValueAtTime(0.0001, start + duration);
    osc.connect(gain);
    gain.connect(context.destination);
    osc.start(start);
    osc.stop(start + duration + 0.02);
  }

  var API = {
    isEnabled: function () { return enabled; },
    setEnabled: function (value) {
      enabled = !!value;
      try { window.localStorage.setItem(STORAGE_KEY, enabled ? "on" : "off"); } catch (error) { /* ignore */ }
      if (enabled) { API.click(); }
      return enabled;
    },
    toggle: function () { return API.setEnabled(!enabled); },
    unlock: function () { var context = audio(); if (context && context.state === "suspended") { context.resume(); } },
    click: function () { tone(520, 0.05, { gain: 0.035 }); },
    coin: function () {
      tone(880, 0.07, { gain: 0.05 });
      tone(1320, 0.12, { gain: 0.05, delay: 0.07 });
    },
    print: function () {
      for (var i = 0; i < 5; i += 1) {
        tone(200 + i * 30, 0.04, { gain: 0.03, delay: i * 0.06, type: "sawtooth" });
      }
    },
    hop: function () { tone(300, 0.12, { gain: 0.05, slideTo: 900, type: "triangle" }); },
    pop: function () { tone(700, 0.06, { gain: 0.04, slideTo: 1100, type: "triangle" }); },
    levelUp: function () {
      var notes = [523, 659, 784, 1046];
      for (var i = 0; i < notes.length; i += 1) {
        tone(notes[i], 0.12, { gain: 0.05, delay: i * 0.1 });
      }
    },
    error: function () { tone(180, 0.16, { gain: 0.05, slideTo: 90, type: "sawtooth" }); }
  };

  window.ObshakSfx = API;
})();
