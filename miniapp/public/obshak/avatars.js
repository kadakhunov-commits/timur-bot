/* Весь пиксель-арт общака рисуется кодом: аватары 16×16, кот, кубки 8×8,
   предметы полки. Никаких картинок — можно править числа и сразу видеть. */
(function () {
  "use strict";

  var PALETTE = {
    skin: "#f2c9a0",
    skinShade: "#d9a97f",
    eye: "#23262f",
    mouth: "#b3555a",
    hairDark: "#2b2f3d",
    beard: "#7a5238",
    shoes: "#23262f",
    furniture: "#424860",
    rustem: "#3ddc97",
    rustemDark: "#249668",
    kadyr: "#ffb347",
    kadyrDark: "#cb8222",
    dilyara: "#ff6b9d",
    dilyaraDark: "#cb3e70",
    amir: "#6bb8ff",
    amirDark: "#3a88d0",
    catBody: "#9ca5ba",
    catDark: "#70798e",
    catEye: "#ffc454"
  };

  // --- аватары --------------------------------------------------------
  // Прямоугольники: [x, y, ширина, высота, цвет]. Сетка 16×16.

  function head() {
    return [[5, 3, 6, 1, "skin"], [4, 4, 8, 6, "skin"], [5, 10, 6, 1, "skin"]];
  }

  function face() {
    return [
      [3, 6, 1, 2, "skin"], [12, 6, 1, 2, "skin"],
      [5, 6, 2, 2, "eye"], [9, 6, 2, 2, "eye"],
      [7, 9, 2, 1, "mouth"],
      [4, 10, 8, 1, "skin"]
    ];
  }

  function body(cloth, dark) {
    return [
      [3, 12, 10, 1, dark],
      [3, 13, 10, 2, cloth],
      [2, 13, 1, 2, dark], [13, 13, 1, 2, dark],
      [5, 15, 2, 1, "shoes"], [9, 15, 2, 1, "shoes"],
      [6, 11, 4, 1, "skinShade"]
    ];
  }

  var AVATARS = {
    rustem: head().concat(face(), body("rustem", "rustemDark"), [
      [4, 1, 8, 2, "hairDark"], [4, 3, 6, 1, "hairDark"],
      [3, 1, 1, 1, "hairDark"], [12, 1, 1, 1, "hairDark"],
      [4, 5, 3, 1, "rustemDark"], [9, 5, 3, 1, "rustemDark"], [7, 5, 2, 1, "rustemDark"],
      [3, 5, 1, 2, "rustemDark"], [12, 5, 1, 2, "rustemDark"]
    ]),
    kadyr: head().concat(face(), body("kadyr", "kadyrDark"), [
      [3, 1, 10, 3, "kadyr"], [3, 4, 10, 1, "kadyrDark"],
      [4, 7, 1, 3, "beard"], [11, 7, 1, 3, "beard"],
      [6, 8, 4, 1, "beard"], [5, 10, 6, 1, "beard"]
    ]),
    dilyara: head().concat(face(), body("dilyara", "dilyaraDark"), [
      [4, 1, 8, 1, "dilyara"], [3, 2, 10, 2, "dilyara"],
      [3, 4, 1, 4, "dilyara"], [12, 4, 1, 4, "dilyara"],
      [13, 4, 2, 5, "dilyara"], [15, 6, 1, 3, "dilyaraDark"],
      [3, 2, 3, 1, "dilyaraDark"]
    ]),
    amir: head().concat(face(), body("amir", "amirDark"), [
      [3, 1, 10, 2, "amir"], [2, 2, 1, 3, "amir"], [13, 2, 1, 3, "amir"],
      [3, 3, 2, 1, "amir"], [11, 3, 2, 1, "amir"],
      [3, 0, 1, 1, "amir"], [6, 0, 1, 1, "amir"], [9, 0, 1, 1, "amir"], [12, 0, 1, 1, "amir"],
      [4, 2, 2, 1, "amirDark"], [10, 2, 2, 1, "amirDark"]
    ])
  };

  var FALLBACK_AVATAR = AVATARS.amir;

  function catRects(sleeping) {
    var rects = [
      [3, 7, 9, 4, "catBody"],
      [1, 4, 6, 5, "catBody"],
      [1, 3, 2, 1, "catBody"], [4, 3, 2, 1, "catBody"],
      [1, 4, 1, 1, "catDark"], [4, 4, 1, 1, "catDark"],
      [12, 8, 1, 2, "catBody"], [13, 6, 1, 3, "catBody"],
      [11, 11, 1, 1, "catDark"], [4, 11, 1, 1, "catDark"],
      [4, 8, 1, 1, "catDark"], [6, 8, 1, 1, "catDark"], [8, 8, 1, 1, "catDark"],
      [3, 7, 1, 1, "dilyara"]
    ];
    if (sleeping) {
      rects.push([2, 5, 1, 1, "catDark"], [5, 5, 1, 1, "catDark"]);
    } else {
      rects.push([2, 5, 1, 1, "catEye"], [5, 5, 1, 1, "catEye"]);
    }
    return rects;
  }

  // --- кубки и предметы: карты 8×8 ------------------------------------

  function mapToRects(rows) {
    var rects = [];
    for (var y = 0; y < rows.length; y += 1) {
      var row = rows[y];
      var x = 0;
      while (x < row.length) {
        var ch = row[x];
        if (ch === ".") { x += 1; continue; }
        var width = 1;
        while (x + width < row.length && row[x + width] === ch) { width += 1; }
        rects.push([x, y, width, 1, ch]);
        x += width;
      }
    }
    return rects;
  }

  var MAPS = {
    coin: [
      "..mmmm..",
      ".mmmmmm.",
      "mmwwwwmm",
      "mmwmmwmm",
      "mmwmmwmm",
      "mmwwwwmm",
      ".mmmmmm.",
      "..mmmm.."
    ],
    crown: [
      "........",
      "m..m..m.",
      "m.mm.m.m",
      "mmmmmmmm",
      "mwwmmwwm",
      "mwwmmwwm",
      "mmmmmmmm",
      "........"
    ],
    star: [
      "...mm...",
      "...mm...",
      "..mmmm..",
      "mmmmmmmm",
      ".mmmmmm.",
      "..mmmm..",
      ".mm..mm.",
      "m......m"
    ],
    whale: [
      "........",
      "...mmmm.",
      ".mmmwmm.",
      "mmmmmmmm",
      "mmmmmmm.",
      "mmdmmd..",
      ".mmmmm..",
      "..mmm..."
    ],
    medal: [
      "..mmmm..",
      ".mwwwwm.",
      ".mwwwwm.",
      "..mmmm..",
      ".mm..mm.",
      "mmmmmmmm",
      "mm.mm.mm",
      "mmm..mmm"
    ],
    box: [
      "........",
      "..dddd..",
      ".dmmmmd.",
      "dmmmmmmd",
      "dmmmmmmd",
      "dmmmmmmd",
      "dmmmmmmd",
      ".dddddd."
    ],
    flame: [
      "...m....",
      "..mm....",
      "..mmm...",
      ".mmmmm..",
      "mmwmmmm.",
      "mmwwmmm.",
      ".mmmmm..",
      "..mmm..."
    ],
    moon: [
      "..mmm...",
      ".mmm....",
      "mmm.....",
      "mmm.....",
      "mmm.....",
      "mmm.....",
      ".mmm....",
      "..mmm..."
    ],
    sun: [
      "m..m..m.",
      ".mmmmmm.",
      ".mwwwwm.",
      "mmwwwwmm",
      "mmwwwwmm",
      ".mwwwwm.",
      ".mmmmmm.",
      "m..m..m."
    ],
    calendar: [
      "m......m",
      "mmmmmmmm",
      "mwmwmwm.",
      "mwmwmwm.",
      "mwmwmwm.",
      "mwmwmwm.",
      "mmmmmmmm",
      "........"
    ],
    trophy: [
      ".mmmmmm.",
      "mwwwwwwm",
      "mwwwwwwm",
      ".mmmmmm.",
      "..mmmm..",
      "...mm...",
      "..mmmm..",
      ".mmmmmm."
    ],
    heart: [
      ".mm..mm.",
      "mmmmmmmm",
      "mmmmmmmm",
      "mmmmmmmm",
      ".mmmmmm.",
      "..mmmm..",
      "...mm...",
      "........"
    ],
    calc: [
      "mmmmmmmm",
      "mwwwwwwm",
      "mmmmmmmm",
      "mwmwmwmm",
      "mwmwmwmm",
      "mwmwmwmm",
      "mmmmmmmm",
      "........"
    ],
    receipt: [
      ".mmmmmm.",
      ".mwmmwm.",
      ".mmmmmm.",
      ".mwmmwm.",
      ".mmmmmm.",
      ".mwmmwm.",
      ".mmmmmm.",
      "..mmmm.."
    ],
    note: [
      ".mmmmmm.",
      "mwwwwwwm",
      "mwwwwwwm",
      "mwwwwwwm",
      "mwwwwwwm",
      "mwwwwwwm",
      ".mmmmmm.",
      "..m..m.."
    ]
  };

  function shade(hex, amount) {
    var value = String(hex || "#000000").replace("#", "");
    if (value.length === 3) {
      value = value[0] + value[0] + value[1] + value[1] + value[2] + value[2];
    }
    var num = parseInt(value, 16);
    if (isNaN(num)) { return hex; }
    var r = Math.max(0, Math.min(255, ((num >> 16) & 255) + amount));
    var g = Math.max(0, Math.min(255, ((num >> 8) & 255) + amount));
    var b = Math.max(0, Math.min(255, (num & 255) + amount));
    return "#" + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);
  }

  function rectsMarkup(rects, colorOf) {
    var out = "";
    for (var i = 0; i < rects.length; i += 1) {
      var rect = rects[i];
      var fill = colorOf ? colorOf(rect[4]) : (PALETTE[rect[4]] || rect[4]);
      if (!fill) { continue; }
      out += '<rect x="' + rect[0] + '" y="' + rect[1] + '" width="' + rect[2] + '" height="' + rect[3] + '" fill="' + fill + '"/>';
    }
    return out;
  }

  function svg(viewBox, markup, size, cls, extra) {
    return '<svg class="' + (cls || "") + '" viewBox="0 0 ' + viewBox + ' ' + viewBox + '" width="' + size + '" height="' + size +
      '" shape-rendering="crispEdges" ' + (extra || "") + '>' + markup + "</svg>";
  }

  function avatarData(key) {
    return AVATARS[key] || FALLBACK_AVATAR;
  }

  function avatarRects(key) {
    return rectsMarkup(avatarData(key));
  }

  function avatar(key, options) {
    options = options || {};
    var size = options.size || 32;
    return svg(16, avatarRects(key), size, options.cls || "avatar-art", options.extra || "");
  }

  function cat(sleeping, options) {
    options = options || {};
    var size = options.size || 32;
    return svg(16, rectsMarkup(catRects(sleeping)), size, options.cls || "", options.extra || "");
  }

  function badge(icon, options) {
    options = options || {};
    var size = options.size || 24;
    var main = options.color || "#ffd447";
    var colors = { m: main, d: shade(main, -70), w: options.contrast || "#2a2205" };
    var rects = mapToRects(MAPS[icon] || MAPS.star);
    return svg(8, rectsMarkup(rects, function (ch) { return colors[ch]; }), size, options.cls || "", options.extra || "");
  }

  function iconMarkup(name, color) {
    var colors = { m: color || "#ffd447", d: shade(color || "#ffd447", -70), w: "#2a2205" };
    return rectsMarkup(mapToRects(MAPS[name] || MAPS.box), function (ch) { return colors[ch]; });
  }

  function brand() {
    return svg(8, iconMarkup("coin", "#ffd447"), 16, "brand-art");
  }

  function crownMarkup(size, color) {
    var colors = { m: color || "#ffd447", d: shade(color || "#ffd447", -70), w: "#2a2205" };
    return rectsMarkup(mapToRects(MAPS.crown), function (ch) { return colors[ch]; });
  }

  window.ObshakArt = {
    PALETTE: PALETTE,
    map: MAPS,
    mapToRects: mapToRects,
    rectsMarkup: rectsMarkup,
    shade: shade,
    avatarData: avatarData,
    avatarRects: avatarRects,
    avatar: avatar,
    catRects: catRects,
    cat: cat,
    badge: badge,
    iconMarkup: iconMarkup,
    crownMarkup: crownMarkup,
    brand: brand
  };
})();
