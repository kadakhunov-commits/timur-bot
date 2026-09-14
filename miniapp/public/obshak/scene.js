/* Кухня общака: сцена рисуется одним SVG в координатах 240×200.
   Персонажи стоят на стопках ящиков — высота стопки и есть вклад. */
(function () {
  "use strict";

  var W = 240;
  var H = 200;
  var FLOOR = 160;
  var CENTERS = [62, 100, 138, 176];

  function esc(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function rect(x, y, w, h, fill, extra) {
    return '<rect x="' + x + '" y="' + y + '" width="' + w + '" height="' + h + '" fill="' + fill + '"' + (extra || "") + "/>";
  }

  function text(x, y, content, fill, size, anchor, weight) {
    return '<text x="' + x + '" y="' + y + '" fill="' + fill + '" font-size="' + size +
      '" text-anchor="' + (anchor || "middle") + '" font-family="var(--f-body)" font-weight="' + (weight || 700) +
      '" class="scene-text">' + esc(content) + "</text>";
  }

  function money(value) {
    var number = Number(value) || 0;
    var whole = Math.abs(number - Math.round(number)) < 0.005;
    var text = whole ? String(Math.round(number)) : String(Math.round(number * 100) / 100);
    return text.replace(/\B(?=(\d{3})+(?!\d))/g, "\u00a0");
  }

  // Суммы на кухне рисуем пиксельными глифами 5×7, а не шрифтом: SVG
  // масштабируется дробно, и текстовые цифры «плывут» — пятёрка читалась
  // как S. Прямоугольники остаются чёткими при любом масштабе.
  var DIGITS = {
    "0": ["01110", "10001", "10011", "10101", "11001", "10001", "01110"],
    "1": ["00100", "01100", "00100", "00100", "00100", "00100", "01110"],
    "2": ["01110", "10001", "00001", "00010", "00100", "01000", "11111"],
    "3": ["11111", "00010", "00100", "00010", "00001", "10001", "01110"],
    "4": ["00010", "00110", "01010", "10010", "11111", "00010", "00010"],
    "5": ["11111", "10000", "11110", "00001", "00001", "10001", "01110"],
    "6": ["00110", "01000", "10000", "11110", "10001", "10001", "01110"],
    "7": ["11111", "00001", "00010", "00100", "01000", "01000", "01000"],
    "8": ["01110", "10001", "10001", "01110", "10001", "10001", "01110"],
    "9": ["01110", "10001", "10001", "01111", "00001", "00010", "01100"],
    ".": ["00000", "00000", "00000", "00000", "00000", "01100", "01100"],
    ",": ["00000", "00000", "00000", "00000", "01100", "00100", "01000"],
    "-": ["00000", "00000", "00000", "11111", "00000", "00000", "00000"],
    // Разделитель тысяч — узкий пробел, чтобы «2 280» не растягивалось.
    "\u00a0": ["00", "00", "00", "00", "00", "00", "00"],
    " ": ["000", "000", "000", "000", "000", "000", "000"]
  };

  function pixelNumber(value, x, baselineY, fill, scale) {
    scale = scale || 1.15;
    var gap = scale;
    var text = String(value);
    var widths = [];
    var totalWidth = 0;
    for (var index = 0; index < text.length; index += 1) {
      var glyph = DIGITS[text[index]];
      var width = (glyph ? glyph[0].length : 3) * scale;
      widths.push(width);
      totalWidth += width;
    }
    totalWidth += gap * Math.max(0, text.length - 1);

    var cursor = x - totalWidth / 2;
    var out = "";
    for (var position = 0; position < text.length; position += 1) {
      var rows = DIGITS[text[position]];
      if (rows) {
        for (var row = 0; row < rows.length; row += 1) {
          var run = 0;
          for (var column = 0; column <= rows[row].length; column += 1) {
            var on = column < rows[row].length && rows[row][column] === "1";
            if (on) { run += 1; continue; }
            if (run > 0) {
              out += rect(
                cursor + (column - run) * scale,
                baselineY - 7 * scale + row * scale,
                run * scale,
                scale,
                fill
              );
              run = 0;
            }
          }
        }
      }
      cursor += widths[position] + gap;
    }
    return out;
  }

  function hotspot(x, y, w, h, action, label) {
    return '<rect class="hotspot" x="' + x + '" y="' + y + '" width="' + w + '" height="' + h +
      '" data-action="' + action + '" role="button" tabindex="0" aria-label="' + esc(label) + '"/>';
  }

  function windowWeather() {
    var clouds = ['<g class="weather-clouds">',
      rect(16, 20, 12, 5, "#c3ccdd"),
      rect(24, 18, 14, 7, "#c3ccdd"),
      rect(34, 22, 12, 5, "#c3ccdd"),
      rect(18, 31, 18, 5, "#d5dcea"),
      rect(30, 29, 16, 6, "#d5dcea"),
      "</g>"];
    var rain = ['<g class="weather-rain">'];
    for (var i = 0; i < 8; i += 1) {
      var x = 17 + i * 5;
      for (var k = 0; k < 4; k += 1) {
        rain.push(rect(x, 16 + k * 12, 1, 5, "#bcd6f5"));
      }
    }
    rain.push("</g>");
    var snow = ['<g class="weather-snow">'];
    for (var j = 0; j < 9; j += 1) {
      var sx = 17 + j * 4 + (j % 3);
      for (var m = 0; m < 3; m += 1) {
        snow.push(rect(sx, 16 + m * 24 + (j % 4) * 3, 2, 2, "#eef4ff"));
      }
    }
    snow.push("</g>");
    var fog = ['<g class="weather-fog">',
      rect(15, 26, 38, 5, "#cfd6e6", ' opacity="0.4"'),
      rect(15, 36, 38, 4, "#cfd6e6", ' opacity="0.32"'),
      rect(15, 45, 38, 5, "#cfd6e6", ' opacity="0.4"'),
      "</g>"];
    var flash = '<g class="weather-flash">' + rect(15, 17, 38, 38, "#ffffff") + "</g>";
    return clouds.join("") + rain.join("") + snow.join("") + fog.join("") + flash;
  }

  function windowFrame() {
    return [
      '<defs><clipPath id="windowClip"><rect x="15" y="17" width="38" height="38"/></clipPath></defs>',
      rect(12, 14, 44, 44, "#4a5164"),
      '<g clip-path="url(#windowClip)">',
      rect(15, 17, 38, 38, "var(--sky)"),
      rect(20, 22, 11, 11, "var(--sky-2)", ' class="scene-sun"'),
      rect(42, 18, 9, 9, "#f4f0d8", ' class="scene-moon"'),
      windowWeather(),
      "</g>",
      rect(33, 17, 2, 38, "#4a5164"),
      rect(15, 34, 38, 2, "#4a5164"),
      rect(12, 58, 44, 3, "#3b4152")
    ].join("");
  }

  function bulb() {
    return [
      rect(119, 0, 2, 12, "#3b4152"),
      '<circle class="bulb-glow" cx="120" cy="24" r="26" fill="#ffd447" opacity="0.15"/>',
      rect(114, 12, 12, 3, "#8b93a3"),
      rect(115, 15, 10, 8, "#d7dde8", ' class="bulb-body"'),
      rect(116, 23, 8, 2, "#b9c2cf")
    ].join("");
  }

  function lightSwitch() {
    return [
      rect(214, 64, 20, 26, "#3b4152"),
      rect(216, 66, 16, 22, "#d7dde8"),
      '<g class="switch-nub">' + rect(220, 69, 8, 8, "#3ddc97") + "</g>"
    ].join("");
  }

  function shoppingBag() {
    return [
      rect(20, 157, 3, 10, "#8a4a24"),
      rect(29, 157, 3, 10, "#8a4a24"),
      rect(14, 165, 24, 27, "#c96f3c"),
      rect(14, 165, 24, 3, "#e08b52"),
      rect(14, 192, 24, 2, "#8a4a24"),
      rect(18, 173, 16, 2, "#8a4a24"),
      rect(18, 180, 12, 2, "#8a4a24")
    ].join("");
  }

  function newspaper() {
    return [
      rect(62, 18, 42, 40, "#efe7d4"),
      rect(66, 22, 34, 6, "#2b2f3d"),
      rect(66, 32, 34, 2, "#b9ab8c"),
      rect(66, 37, 28, 2, "#b9ab8c"),
      rect(66, 42, 32, 2, "#b9ab8c"),
      rect(66, 47, 22, 2, "#b9ab8c"),
      rect(60, 16, 46, 2, "#8b93a3")
    ].join("");
  }

  function trophyShelf() {
    var art = window.ObshakArt;
    var colors = ["#ffd447", "#c7cdd8", "#cd8a5a"];
    var out = [rect(112, 50, 56, 6, "#7a5238"), rect(112, 50, 56, 2, "#9a6a48")];
    for (var i = 0; i < 3; i += 1) {
      out.push('<svg x="' + (116 + i * 17) + '" y="36" width="14" height="14" viewBox="0 0 8 8">' +
        art.rectsMarkup(art.mapToRects(art.map.trophy), (function (color) {
          return function (ch) {
            if (ch === "m") { return color; }
            if (ch === "d") { return art.shade(color, -70); }
            return "#2a2205";
          };
        })(colors[i])) + "</svg>");
    }
    return out.join("");
  }

  function corkboard(count) {
    var out = [rect(176, 14, 56, 50, "#6b4a34"), rect(180, 18, 48, 42, "#a9825a")];
    var total = Math.min(count, 6);
    for (var i = 0; i < total; i += 1) {
      var col = i % 3;
      var row = Math.floor(i / 3);
      var x = 184 + col * 15;
      var y = 21 + row * 19 + (i % 2 ? 2 : 0);
      out.push(rect(x, y, 12, 16, "#f4efe0"));
      out.push(rect(x + 1, y + 2, 10, 1, "#b9ab8c"));
      out.push(rect(x + 1, y + 5, 8, 1, "#b9ab8c"));
      out.push(rect(x + 1, y + 8, 10, 1, "#b9ab8c"));
      out.push(rect(x + 5, y - 1, 2, 2, "#e05a5a"));
    }
    return out.join("");
  }

  function fridge(magnets, notes) {
    var out = [
      rect(6, 66, 48, 92, "#d7dde8"),
      rect(6, 66, 48, 4, "#b9c2cf"),
      rect(6, 104, 48, 2, "#b9c2cf"),
      rect(48, 80, 3, 18, "#8b93a3"),
      rect(48, 116, 3, 26, "#8b93a3")
    ];
    var colors = ["#ff6b9d", "#3ddc97", "#ffb347", "#6bb8ff", "#ffd447", "#c37bff"];
    var total = Math.min(magnets, 6);
    for (var i = 0; i < total; i += 1) {
      var x = 11 + (i % 3) * 13;
      var y = 73 + Math.floor(i / 3) * 13;
      out.push(rect(x, y, 8, 8, colors[i % colors.length]));
      out.push(rect(x, y, 8, 2, "rgba(255,255,255,.45)"));
    }
    if (notes > 0) {
      // Записки на дверце: их видно, даже если дверцу не открывать.
      out.push('<g class="fridge-notes">');
      for (var n = 0; n < Math.min(notes, 3); n += 1) {
        var ny = 88 + n * 5;
        out.push(rect(12, ny, 14, 4, n % 2 ? "#f4efe0" : "#ffd447", ' opacity="0.92"'));
      }
      out.push("</g>");
    }
    return out.join("");
  }

  function table() {
    return [
      rect(104, 112, 108, 8, "#7a5238"),
      rect(104, 120, 108, 3, "#5d3f2b"),
      rect(110, 123, 6, 37, "#5d3f2b"),
      rect(200, 123, 6, 37, "#5d3f2b")
    ].join("");
  }

  function jar(progress) {
    var innerTop = 88;
    var innerBottom = 111;
    var height = Math.max(0, Math.round((innerBottom - innerTop) * Math.max(0, Math.min(1, progress))));
    return [
      '<g class="jar-wrap">',
      rect(152, 80, 20, 5, "#8b93a3"),
      rect(150, 85, 24, 28, "rgba(220,228,244,.14)"),
      rect(150, 85, 24, 2, "rgba(220,228,244,.5)"),
      rect(150, 111, 24, 2, "rgba(220,228,244,.5)"),
      rect(150, 85, 2, 28, "rgba(220,228,244,.5)"),
      rect(172, 85, 2, 28, "rgba(220,228,244,.5)"),
      height > 0 ? rect(152, innerBottom - height, 20, height, "#ffd447", ' class="jar-fill"') : "",
      height > 0 ? rect(152, innerBottom - height, 20, 1, "#fff0b0") : "",
      rect(154, 89, 3, 14, "rgba(255,255,255,.25)"),
      "</g>"
    ].join("");
  }

  function catBlock(sleeping) {
    var art = window.ObshakArt;
    return '<g class="cat-wrap" transform="translate(196,158.8) scale(1.6)">' +
      '<g class="avatar-art cat-art">' + art.rectsMarkup(art.catRects(sleeping)) + "</g></g>";
  }

  function pedestal(cx, stackHeight, color) {
    var rows = Math.max(1, Math.round(stackHeight / 6));
    var out = "";
    for (var i = 0; i < rows; i += 1) {
      var y = FLOOR - (i + 1) * 6;
      out += rect(cx - 13, y, 26, 6, i % 2 === 0 ? "#5a4633" : "#473827");
      out += rect(cx - 13, y, 26, 1, "#6f5740");
    }
    var topY = FLOOR - stackHeight - 6;
    out += rect(cx - 14, topY, 28, 6, color);
    out += rect(cx - 14, topY, 28, 1, "rgba(255,255,255,.3)");
    return out;
  }

  function character(member, total, maxTotal, index, isMe, animate, crown) {
    var art = window.ObshakArt;
    var cx = CENTERS[index % CENTERS.length];
    var ratio = maxTotal > 0 ? Math.max(0, Math.min(1, total / maxTotal)) : 0;
    var stackHeight = 10 + 46 * ratio;
    var topY = FLOOR - stackHeight;
    var avatarY = topY - 30;
    var leader = maxTotal > 0 && total >= maxTotal && total > 0;
    var out = ['<g class="character' + (isMe ? " is-me" : "") + (animate ? " walk" : "") +
      '" style="--walk-delay:' + (index * 140 + 120) + 'ms" data-action="member" data-member="' + esc(member.key) +
      '" role="button" tabindex="0" aria-label="' + esc(member.name) + ': ' + money(total) + '">'];
    out.push(rect(cx - 13, 156, 26, 5, "rgba(0,0,0,.25)"));
    out.push('<g class="stack">' + pedestal(cx, stackHeight, member.color) + "</g>");
    out.push('<g transform="translate(' + (cx - 16) + "," + avatarY + ') scale(2)"><g class="avatar-art">' +
      art.avatarRects(member.key) + "</g></g>");
    if (leader) {
      out.push('<g transform="translate(' + (cx - 9) + "," + (avatarY - 13) + ')">' +
        '<g class="crown" transform="scale(2)">' + art.crownMarkup() + "</g></g>");
    }
    if (crown) {
      // Корона месяца: золотая и выше обычной — видно, кто держит месяц.
      out.push('<g transform="translate(' + (cx - 11) + "," + (avatarY - 25) + ')">' +
        '<g class="crown crown-month" transform="scale(2.4)">' + art.crownMarkup(null, "#ffd447") + "</g></g>");
    }
    // Имя — прописными: у строчных «д» и «ы» в пиксельном шрифте кривые начертания.
    out.push(text(cx, 171, String(member.name).toUpperCase(), member.color, 8, "middle", 700));
    out.push(pixelNumber(money(total), cx, 182, "#eef1f7"));
    out.push("</g>");
    return out.join("");
  }

  function build(ctx, animate) {
    var total = 0;
    var rows = ctx.leaderboard || [];
    var byKey = {};
    rows.forEach(function (row) { byKey[row.member_id] = row; });
    var maxTotal = rows.reduce(function (max, row) { return Math.max(max, row.total || 0); }, 0);
    var members = ctx.members || [];

    var parts = [
      rect(0, 0, W, FLOOR, "var(--wall)"),
      rect(0, 120, W, 40, "var(--wall-2)"),
      rect(0, FLOOR, W, H - FLOOR, "var(--floor)")
    ];
    for (var x = 0; x <= W; x += 20) {
      parts.push(rect(x, FLOOR, 1, H - FLOOR, "rgba(0,0,0,.16)"));
    }
    parts.push(rect(0, FLOOR + 12, W, 1, "rgba(0,0,0,.16)"));

    parts.push(windowFrame());
    parts.push(bulb());
    parts.push(newspaper());
    parts.push(trophyShelf());
    parts.push(corkboard((ctx.recentCount || 0)));
    parts.push(lightSwitch());
    parts.push(fridge(ctx.wishlistCount || 0, (ctx.notes || []).length));
    parts.push(shoppingBag());
    parts.push(table());
    parts.push(jar((ctx.jar && ctx.jar.progress) || 0));
    parts.push(catBlock(!!(ctx.pet && ctx.pet.sleeping)));

    // Хотспоты рисуем до персонажей: в наложении клик забирает персонаж.
    parts.push(hotspot(12, 14, 44, 44, "calendar", "Календарь активности"));
    parts.push(hotspot(58, 16, 46, 44, "news", "Соседские новости"));
    parts.push(hotspot(112, 34, 56, 22, "achievements", "Зал славы"));
    parts.push(hotspot(176, 14, 56, 50, "history", "Чеки"));
    parts.push(hotspot(6, 66, 48, 92, "wishlist", "Надо купить"));
    parts.push(hotspot(148, 78, 28, 36, "jar", "Общая банка"));
    parts.push(hotspot(194, 156, 34, 30, "pet", "Кот"));
    parts.push(hotspot(10, 152, 34, 46, "roulette", "Кто идёт в магазин"));
    parts.push(hotspot(210, 60, 28, 34, "light", "Выключатель"));

    var crownKey = (ctx.crown && ctx.crown.member_id) || null;
    members.forEach(function (member, index) {
      var row = byKey[member.key] || { total: 0, count: 0 };
      total += row.total || 0;
      parts.push(character(member, row.total || 0, maxTotal, index, ctx.me === member.key, animate, crownKey === member.key));
    });

    return '<svg class="scene" viewBox="0 0 ' + W + " " + H + '" preserveAspectRatio="xMidYMid meet" aria-label="Кухня соседей">' +
      parts.join("") + "</svg>";
  }

  var entered = false;

  function render(container, ctx) {
    // Персонажи заходят домой только при первом рендере за сессию,
    // иначе они будут бегать по кухне после каждой записанной покупки.
    container.innerHTML = build(ctx, !entered);
    entered = true;
  }

  function svgNS() { return "http://www.w3.org/2000/svg"; }

  function animateCoin(container) {
    var wrap = container.querySelector(".jar-wrap");
    if (!wrap) { return; }
    var coin = document.createElementNS(svgNS(), "rect");
    coin.setAttribute("x", "156");
    coin.setAttribute("y", "54");
    coin.setAttribute("width", "8");
    coin.setAttribute("height", "8");
    coin.setAttribute("fill", "#ffd447");
    coin.setAttribute("class", "coin-drop");
    wrap.appendChild(coin);
    window.setTimeout(function () {
      if (coin.parentNode) { coin.parentNode.removeChild(coin); }
    }, 700);
  }

  function hop(container, selector) {
    var node = container.querySelector(selector);
    if (!node) { return; }
    node.classList.remove("hop");
    // Перезапуск анимации: без reflow браузер может её не увидеть.
    void node.offsetWidth;
    node.classList.add("hop");
    window.setTimeout(function () { node.classList.remove("hop"); }, 520);
  }

  function hopMember(container, memberKey) {
    hop(container, '.character[data-member="' + memberKey + '"] .avatar-art');
  }

  function hopCat(container) {
    hop(container, ".cat-art");
  }

  window.ObshakScene = {
    render: render,
    animateCoin: animateCoin,
    hopMember: hopMember,
    hopCat: hopCat,
    money: money
  };
})();
