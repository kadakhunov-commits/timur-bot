/* Общак — логика миниаппа: состояние, API, панели, калькулятор и эффекты. */
(function () {
  "use strict";

  var API = "/api/obshak";
  var tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
  var art = window.ObshakArt;
  var scene = window.ObshakScene;
  var sfx = window.ObshakSfx;
  var share = window.ObshakShare;

  var ERROR_TEXT = {
    needs_link: "Выбери, кто ты",
    slot_taken: "Этот аватар уже занят",
    already_linked: "Ты уже привязан к другому аватару",
    not_a_member: "Тебя нет в беседе соседей",
    not_allowed: "Чужие покупки трогать нельзя",
    already_paid: "Ты уже скинулся",
    not_a_target: "Этот запрос не к тебе",
    item_closed: "Уже закрыто",
    request_closed: "Запрос закрыт",
    not_author: "Закрыть может только автор",
    bad_amount: "Проверь сумму",
    amount_too_big: "Слишком много, кот не поверит",
    empty_title: "Напиши, что купил",
    unknown_member: "Неизвестный участник",
    unknown_expense: "Покупка не найдена",
    unknown_request: "Запрос не найден",
    unknown_wishlist_item: "Пункт не найден",
    bad_reaction: "Неизвестная реакция",
    bad_hash: "Подпись Telegram не сошлась",
    expired: "Сессия устарела, открой миниапп заново",
    missing_init_data: "Открой «Соседей» кнопкой в беседе",
    server_misconfigured: "Сервер не настроен",
    bad_user: "Не удалось определить пользователя"
  };

  var REACTIONS = [
    { key: "fire", icon: "🔥", label: "огонь" },
    { key: "goat", icon: "🐐", label: "козёл" },
    { key: "cry", icon: "😭", label: "слёзы" },
    { key: "clap", icon: "👏", label: "аплодисменты" }
  ];

  var state = {
    data: null,
    period: "all",
    me: null,
    startParam: "",
    historyFilter: null,
    historyCategory: null,
    historyQuery: "",
    historyLimit: 20,
    historyRows: null,
    historyTotal: 0,
    listsTab: "wishlist",
    calendarMember: null,
    calc: null,
    panelSpec: null,
    booted: false,
    bound: false,
    light: "on",
    weather: null,
    roulette: null,
    catTimer: null
  };

  var CATEGORY_ICONS = {
    food: "jar",
    household: "soap",
    drinks: "bottle",
    fun: "spark",
    other: "box"
  };

  var WEATHER_LABEL = {
    clear: "ясно",
    clouds: "облачно",
    rain: "дождь",
    snow: "снег",
    fog: "туман",
    thunder: "гроза"
  };

  var el = {};

  // --- утилиты ---------------------------------------------------------

  function h(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function money(value) {
    var number = Number(value) || 0;
    var whole = Math.abs(number - Math.round(number)) < 0.005;
    // Копейки пишем через запятую: «1 347,06», а не «1 347.06».
    var text = whole ? String(Math.round(number)) : number.toFixed(2).replace(".", ",");
    return text.replace(/\B(?=(\d{3})+(?!\d))/g, "\u00a0");
  }

  function currency() {
    return (state.data && state.data.currency) || "₽";
  }

  function memberByKey(key) {
    var members = (state.data && state.data.members) || [];
    for (var i = 0; i < members.length; i += 1) {
      if (members[i].key === key) { return members[i]; }
    }
    return null;
  }

  function memberName(key) {
    var member = memberByKey(key);
    return member ? member.name : "—";
  }

  function memberColor(key) {
    var member = memberByKey(key);
    return member ? member.color : "#8b90a3";
  }

  function leaderRow(key) {
    var rows = (state.data && state.data.leaderboard) || [];
    for (var i = 0; i < rows.length; i += 1) {
      if (rows[i].member_id === key) { return rows[i]; }
    }
    return { total: 0, count: 0 };
  }

  function parseDate(value) {
    var date = new Date(value);
    return isNaN(date.getTime()) ? null : date;
  }

  function formatWhen(value) {
    var date = parseDate(value);
    if (!date) { return ""; }
    var now = new Date();
    var dayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    var target = new Date(date.getFullYear(), date.getMonth(), date.getDate());
    var diff = Math.round((dayStart - target) / 86400000);
    var time = String(date.getHours()).padStart(2, "0") + ":" + String(date.getMinutes()).padStart(2, "0");
    if (diff === 0) { return "сегодня " + time; }
    if (diff === 1) { return "вчера " + time; }
    var months = ["янв", "фев", "мар", "апр", "май", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"];
    return date.getDate() + " " + months[date.getMonth()] + " " + time;
  }

  function categoryName(key) {
    var categories = (state.data && state.data.categories) || [];
    for (var i = 0; i < categories.length; i += 1) {
      if (categories[i].key === key) { return categories[i].name; }
    }
    return "Прочее";
  }

  function pluralDays(count) {
    var n = Math.abs(Number(count) || 0) % 100;
    var last = n % 10;
    if (n > 10 && n < 20) { return "дней"; }
    if (last === 1) { return "день"; }
    if (last >= 2 && last <= 4) { return "дня"; }
    return "дней";
  }

  function pluralTimes(count) {
    var n = Math.abs(Number(count) || 0) % 100;
    var last = n % 10;
    if (n > 10 && n < 20) { return "раз"; }
    if (last === 1) { return "раз"; }
    if (last >= 2 && last <= 4) { return "раза"; }
    return "раз";
  }

  function weatherLabel(weather) {
    if (!weather) { return ""; }
    var kind = WEATHER_LABEL[weather.kind] || "";
    var temp = (weather.temp === null || weather.temp === undefined) ? "" : weather.temp + "°";
    return [temp, kind].filter(Boolean).join(", ");
  }

  function questProgressText(quest) {
    if (!quest) { return ""; }
    if (quest.kind === "money") {
      return money(quest.progress) + " / " + money(quest.target) + "\u00a0" + currency();
    }
    return quest.progress + " из " + quest.target;
  }

  function haptic(kind) {
    if (!tg || !tg.HapticFeedback) { return; }
    try {
      if (kind === "success" || kind === "error") { tg.HapticFeedback.notificationOccurred(kind); }
      else { tg.HapticFeedback.impactOccurred(kind || "light"); }
    } catch (error) { /* ignore */ }
  }

  function timeOfDay() {
    var hour = new Date().getHours();
    if (hour < 6) { return "night"; }
    if (hour < 11) { return "morning"; }
    if (hour < 18) { return "day"; }
    if (hour < 23) { return "evening"; }
    return "night";
  }

  function toast(text, options) {
    options = options || {};
    var node = document.createElement("div");
    node.className = "toast" + (options.type === "error" ? " is-error" : "");
    node.innerHTML = "<span>" + h(text) + "</span>";
    if (options.actionLabel && options.onAction) {
      var button = document.createElement("button");
      button.type = "button";
      button.textContent = options.actionLabel;
      button.addEventListener("click", function () {
        options.onAction();
        node.remove();
      });
      node.appendChild(button);
    }
    el.toastRoot.appendChild(node);
    window.setTimeout(function () { node.remove(); }, options.duration || 2600);
  }

  // --- API --------------------------------------------------------------

  function api(path, options) {
    options = options || {};
    var headers = { "X-Telegram-Init-Data": (tg && tg.initData) || "" };
    if (options.body) { headers["Content-Type"] = "application/json"; }
    return fetch(API + path, {
      method: options.method || "GET",
      headers: headers,
      body: options.body ? JSON.stringify(options.body) : undefined
    }).then(function (response) {
      return response.json().catch(function () { return { ok: false, error: "network" }; })
        .then(function (payload) {
          if (!response.ok || !payload.ok) {
            var error = new Error(payload.error || "network");
            error.code = payload.error || "network";
            error.payload = payload;
            error.status = response.status;
            throw error;
          }
          return payload;
        });
    });
  }

  function fail(error) {
    if (error && error.code === "needs_link") {
      state.booted = true;
      renderPicker(error.payload && error.payload.members);
      return;
    }
    sfx.error();
    haptic("error");
    var text = ERROR_TEXT[error && error.code] || "Что-то сломалось, попробуй ещё";
    if (error && error.code === "missing_init_data") { showBootError(text); return; }
    toast(text, { type: "error" });
  }

  function refresh() {
    return api("/bootstrap?period=" + encodeURIComponent(state.period)).then(function (payload) {
      var previousLevel = state.data && state.data.jar ? state.data.jar.level.index : null;
      state.data = payload;
      state.me = payload.me;
      state.booted = true;
      hideBoot();
      renderChrome();
      renderScene();
      drawPanel();
      syncLight();
      if (previousLevel !== null && payload.jar.level.index > previousLevel) {
        celebrate(payload.jar.level.name);
      }
      return payload;
    });
  }

  function syncLight() {
    // Свет сервера общий: если сосед выключил — увидим все.
    var light = state.data && state.data.light;
    if (!light) { return; }
    var next = light.on ? "on" : "off";
    if (next === state.light) { return; }
    state.light = next;
    try { window.localStorage.setItem("obshak.light", next); } catch (error) { /* ignore */ }
    applyLight();
  }

  // --- каркас -----------------------------------------------------------

  function renderChrome() {
    el.app.setAttribute("data-time", timeOfDay());
    el.totalValue.textContent = money(state.data.totals_period.total);
    el.totalCur.textContent = currency();
    renderShelf();
    renderHomeBars();
    renderStrip();
    renderBudget();
    Array.prototype.forEach.call(el.periodBar.querySelectorAll(".period"), function (button) {
      button.classList.toggle("is-active", button.dataset.period === state.period);
    });
    el.soundBtn.classList.toggle("is-off", !sfx.isEnabled());
    el.soundBtn.textContent = sfx.isEnabled() ? "♪" : "×";
  }

  function renderBudget() {
    if (!el.budgetBar) { return; }
    var budget = (state.data && state.data.budget) || {};
    if (!budget.enabled) { el.budgetBar.hidden = true; return; }
    el.budgetBar.hidden = false;
    var percent = Math.min(100, Math.round((budget.progress || 0) * 100));
    var over = percent >= 100;
    var warn = percent >= 80;
    el.budgetFill.style.width = percent + "%";
    el.budgetBar.classList.toggle("is-over", over);
    el.budgetBar.classList.toggle("is-warn", warn && !over);
    el.budgetLabel.textContent = over ? "бюджет месяца пробит" : "бюджет месяца";
    el.budgetValue.textContent = money(budget.spent) + " / " + money(budget.limit) + "\u00a0" + currency();
  }

  function renderShelf() {
    var items = [
      { act: "add-open", icon: "calc", label: "ЗАПИСАТЬ" },
      { act: "history-open", icon: "receipt", label: "ЧЕКИ" },
      { act: "lists-open", icon: "note", label: "СПИСКИ" },
      { act: "achievements-open", icon: "trophy", label: "КУБКИ" },
      { act: "stats-open", icon: "chart", label: "ЦИФРЫ" }
    ];
    el.shelf.innerHTML = items.map(function (item) {
      return '<button class="shelf-item" type="button" data-act="' + item.act + '">' +
        art.badge(item.icon, { size: 22, color: "#f0e2cd", contrast: "#3a2b1e" }) +
        '<span class="shelf-label">' + item.label + "</span></button>";
    }).join("");
  }

  function renderHomeBars() {
    var rows = state.data.leaderboard || [];
    var max = rows.reduce(function (acc, row) { return Math.max(acc, row.total); }, 0);
    el.homeBars.innerHTML = '<div class="bars">' + rows.map(function (row) {
      var percent = max > 0 ? Math.max(3, Math.round(100 * row.total / max)) : 3;
      return '<button class="bar" type="button" data-action="member" data-member="' + h(row.member_id) +
        '" aria-label="' + h(row.name) + ": " + money(row.total) + ' ' + currency() + '">' +
        '<span class="bar-value">' + money(row.total) + "</span>" +
        '<div class="bar-track"><div class="bar-fill" style="height:' + percent + "%;background:" + h(row.color) + '"></div></div>' +
        '<span class="bar-foot">' + art.avatar(row.member_id, { size: 18 }) +
        '<span class="bar-name">' + h(String(row.name).toUpperCase()) + "</span></span></button>";
    }).join("") + "</div>";
  }

  function renderStrip() {
    if (!el.statusStrip || !state.data) { return; }
    var streak = state.data.streak || { current: 0, best: 0 };
    var quest = state.data.quest;
    var hot = streak.current > 0;
    var parts = [];
    parts.push(
      '<span class="' + (hot ? "is-hot" : "") + '">' +
      art.badge("flame", { size: 14, color: hot ? "#ffd447" : "#4c5165", contrast: "#2a2205" }) +
      " " + (hot ? streak.current + " " + pluralDays(streak.current) + " подряд" : "соседи затихли") +
      "</span>"
    );
    if (quest) {
      parts.push("<span>КВЕСТ: " + h(questProgressText(quest)) + (quest.done ? " ✓" : "") + "</span>");
    }
    var label = weatherLabel(state.weather);
    if (label) { parts.push("<span>" + h(label) + "</span>"); }
    el.statusStrip.innerHTML = parts.join("<i>·</i>");
    el.statusStrip.classList.toggle("is-hot", hot);
  }

  function applyLight() {
    el.app.setAttribute("data-light", state.light);
  }

  function toggleLight() {
    var next = state.light === "off" ? "on" : "off";
    state.light = next;
    try { window.localStorage.setItem("obshak.light", state.light); } catch (error) { /* ignore */ }
    applyLight();
    sfx.click();
    if (state.light === "off") {
      var phrases = (state.data && state.data.light_phrases) || [];
      if (phrases.length) { toast(phrases[Math.floor(Math.random() * phrases.length)]); }
      scene.hopCat(el.stage);
      haptic("light");
    }
    // Свет общий: выключатель виден всем, кто откроет кухню.
    api("/light", { method: "POST", body: { on: state.light === "on" } })
      .then(function (payload) {
        if (payload.light && state.data) { state.data.light = payload.light; }
      })
      .catch(function () { /* свет — не критично, остаёмся с локальным */ });
  }

  function loadWeather() {
    if (!state.data) { return; }
    api("/weather").then(function (payload) {
      state.weather = payload.weather || null;
      el.app.setAttribute("data-weather", (state.weather && state.weather.kind) || "none");
      renderStrip();
    }).catch(function () {
      // Погода — необязательная роскошь: молча оставляем обычное небо.
      el.app.setAttribute("data-weather", "none");
    });
  }

  function renderScene() {
    var data = state.data;
    scene.render(el.stage, {
      members: data.members,
      leaderboard: data.leaderboard,
      jar: data.jar,
      pet: data.pet,
      me: state.me,
      crown: data.crown,
      notes: data.notes || [],
      wishlistCount: data.wishlist_open.length,
      recentCount: data.recent.length
    });
  }

  function hideBoot() {
    if (el.boot.classList.contains("is-hidden")) { return; }
    el.boot.classList.add("is-hidden");
    window.setTimeout(function () { el.boot.hidden = true; }, 300);
  }

  function showBootError(text) {
    el.boot.hidden = false;
    el.boot.classList.remove("is-hidden");
    el.bootText.textContent = text;
  }

  // --- панели -----------------------------------------------------------

  function setPanel(spec) {
    state.panelSpec = spec;
    drawPanel();
    var body = el.panelRoot.querySelector(".panel-body");
    if (body) { body.scrollTop = 0; }
  }

  function drawPanel() {
    var spec = state.panelSpec;
    if (!spec) { el.panelRoot.hidden = true; el.panelRoot.innerHTML = ""; return; }
    el.panelRoot.hidden = false;
    el.panelRoot.innerHTML =
      '<div class="panel-scrim" data-act="panel-close"></div>' +
      '<div class="panel" role="dialog" aria-label="' + h(spec.title || "") + '">' +
      '<div class="panel-head">' +
      '<button class="panel-back" type="button" data-act="panel-close" aria-label="Назад">◀</button>' +
      '<h2 class="panel-title">' + h(spec.title || "") + "</h2>" +
      (spec.sub ? '<span class="panel-sub">' + h(spec.sub) + "</span>" : "") +
      "</div>" +
      '<div class="panel-body">' + (spec.body ? spec.body() : "") + "</div>" +
      (spec.foot ? '<div class="panel-foot">' + spec.foot() + "</div>" : "") +
      "</div>";
    if (spec.onMount) { spec.onMount(el.panelRoot); }
  }

  function closePanel() {
    state.panelSpec = null;
    drawPanel();
  }

  // --- выбор участника --------------------------------------------------

  function renderPicker(members) {
    members = members || (state.data && state.data.members) || [];
    el.pickerRoot.hidden = false;
    el.pickerRoot.innerHTML =
      '<div class="picker">' +
      '<div class="picker-title">КТО ТЫ</div>' +
      '<p class="picker-sub">Выбери свой аватар — это запомнится, и вклады будут считаться на тебя.</p>' +
      '<div class="picker-grid">' +
      members.map(function (member) {
        return '<button class="picker-card" type="button" data-act="pick" data-member="' + h(member.key) + '">' +
          art.avatar(member.key, { size: 64 }) +
          '<span class="picker-name" style="color:' + h(member.color) + '">' + h(member.name) + "</span>" +
          (member.linked ? '<span class="picker-locked">занят</span>' : "") +
          "</button>";
      }).join("") +
      "</div></div>";
  }

  function pick(memberKey) {
    api("/me", { method: "POST", body: { member_id: memberKey } }).then(function () {
      el.pickerRoot.hidden = true;
      el.pickerRoot.innerHTML = "";
      sfx.levelUp();
      haptic("success");
      if (state.data) { refresh().catch(fail); }
      else { loadInitial(); }
    }).catch(function (error) {
      sfx.error();
      haptic("error");
      toast(ERROR_TEXT[error.code] || "Не получилось выбрать", { type: "error" });
    });
  }

  // --- эффекты ----------------------------------------------------------

  function celebrate(label) {
    sfx.levelUp();
    haptic("success");
    fireworks();
    toast("Новый уровень банки: " + label);
  }

  function fireworks() {
    var canvas = el.fxCanvas;
    var ctx = canvas.getContext("2d");
    var ratio = window.devicePixelRatio || 1;
    canvas.width = window.innerWidth * ratio;
    canvas.height = window.innerHeight * ratio;
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    ctx.imageSmoothingEnabled = false;
    canvas.classList.add("is-on");
    var colors = ["#ffd447", "#3ddc97", "#ff6b9d", "#6bb8ff", "#ffb347"];
    var particles = [];
    for (var i = 0; i < 60; i += 1) {
      var angle = (Math.PI * 2 * i) / 60;
      var speed = 2 + Math.random() * 5;
      particles.push({
        x: window.innerWidth / 2,
        y: window.innerHeight * 0.42,
        vx: Math.cos(angle) * speed,
        vy: Math.sin(angle) * speed - 1.5,
        size: 4 + Math.floor(Math.random() * 4),
        color: colors[i % colors.length],
        life: 1
      });
    }
    var started = performance.now();
    function frame(now) {
      var elapsed = now - started;
      ctx.clearRect(0, 0, window.innerWidth, window.innerHeight);
      particles.forEach(function (particle) {
        particle.vy += 0.16;
        particle.x += particle.vx;
        particle.y += particle.vy;
        particle.life = Math.max(0, 1 - elapsed / 1400);
        ctx.globalAlpha = particle.life;
        ctx.fillStyle = particle.color;
        ctx.fillRect(Math.round(particle.x), Math.round(particle.y), particle.size, particle.size);
      });
      ctx.globalAlpha = 1;
      if (elapsed < 1400) { window.requestAnimationFrame(frame); }
      else { ctx.clearRect(0, 0, window.innerWidth, window.innerHeight); canvas.classList.remove("is-on"); }
    }
    window.requestAnimationFrame(frame);
  }

  // --- калькулятор ------------------------------------------------------

  function newCalc(prefill) {
    prefill = prefill || {};
    var last = state.data && state.data.recent && state.data.recent[0];
    state.calc = {
      amount: prefill.amount ? String(prefill.amount) : "",
      title: prefill.title || "",
      category: prefill.category || "",
      member: state.me,
      categoryTouched: !!prefill.category,
      wishlistId: prefill.wishlistId || null
    };
    if (!state.calc.category) {
      // Категория по умолчанию — как в прошлый раз для такого же названия.
      state.calc.category = guessCategory(state.calc.title) || "other";
    }
    if (!prefill.title && last && last.member_id === state.me) {
      state.calc.last = { title: last.title, amount: last.amount, category: last.category };
    }
    return state.calc;
  }

  function guessCategory(title) {
    var needle = String(title || "").trim().toLowerCase();
    if (!needle) { return null; }
    var rows = (state.data && state.data.recent) || [];
    for (var i = 0; i < rows.length; i += 1) {
      if (String(rows[i].title || "").trim().toLowerCase() === needle && rows[i].category) {
        return rows[i].category;
      }
    }
    return null;
  }

  function categoryIcon(key) {
    var categories = (state.data && state.data.categories) || [];
    for (var i = 0; i < categories.length; i += 1) {
      if (categories[i].key === key) {
        return CATEGORY_ICONS[categories[i].icon] || categories[i].icon || CATEGORY_ICONS[key] || "box";
      }
    }
    return CATEGORY_ICONS[key] || "box";
  }

  function calcValue() {
    var raw = (state.calc.amount || "").replace(",", ".");
    var value = parseFloat(raw);
    return isNaN(value) ? 0 : value;
  }

  function openAdd(prefill) {
    newCalc(prefill);
    setPanel({
      title: state.calc.wishlistId ? "КУПЛЕНО ИЗ СПИСКА" : "ЗАПИСАТЬ ПОКУПКУ",
      body: addBody,
      foot: function () {
        var calc = state.calc || { amount: "", title: "" };
        var ready = calcValue() > 0 && (calc.title || "").trim();
        return '<button class="btn primary block" type="button" data-act="add-save"' + (ready ? "" : " disabled") + ">ЗАПИСАТЬ</button>";
      },
      onMount: function () {
        bindAddInput();
        scrollActiveCategoryIntoView();
      }
    });
  }

  function catChipsHtml(calc, categories) {
    return categories.map(function (category) {
      var active = calc.category === category.key;
      return '<button class="cat-chip' + (active ? " is-active" : "") +
        (calc.categoryTouched ? "" : " is-hint") + '" type="button" data-act="add-cat" data-cat="' + h(category.key) + '"' +
        ' aria-pressed="' + (active ? "true" : "false") + '">' +
        art.badge(categoryIcon(category.key), { size: 20, color: active ? "#20160a" : "#f0e2cd", contrast: active ? "#ffd447" : "#3a2b1e" }) +
        '<span class="cat-chip-name">' + h(category.name) + "</span></button>";
    }).join("");
  }

  function addCategoryRow(calc, categories) {
    // Категорию видно сразу под суммой: раньше её прятал скролл под клавиатурой.
    return '<div class="field cat-field"><span class="field-label">Категория' +
      (calc.categoryTouched ? "" : ' <i class="cat-hint">— выбери или оставь «' + h(categoryName(calc.category)) + '»</i>') +
      '</span><div class="cat-row" role="group" aria-label="Категория">' + catChipsHtml(calc, categories) + "</div></div>";
  }

  function scrollActiveCategoryIntoView() {
    var active = el.panelRoot.querySelector(".cat-chip.is-active");
    if (active && active.scrollIntoView) {
      active.scrollIntoView({ block: "nearest", inline: "center" });
    }
  }

  function refreshCategoryRow() {
    var row = el.panelRoot.querySelector(".cat-row");
    if (!row || !state.calc) { return; }
    row.innerHTML = catChipsHtml(state.calc, (state.data && state.data.categories) || []);
    var hint = el.panelRoot.querySelector(".cat-hint");
    if (hint) { hint.innerHTML = "— выбери или оставь «" + h(categoryName(state.calc.category)) + "»"; }
    scrollActiveCategoryIntoView();
  }

  function addBody() {
    var calc = state.calc;
    var members = state.data.members;
    var quick = state.data.quick_titles || [];
    var categories = state.data.categories || [];
    var keys = ["1", "2", "3", "4", "5", "6", "7", "8", "9", ",", "0", "back"];
    var last = calc.last && !calc.amount && !calc.title
      ? '<button class="btn ghost block" type="button" data-act="add-repeat">повторить: ' + h(calc.last.title) + " — " + money(calc.last.amount) + "\u00a0" + currency() + "</button>"
      : "";
    return "" +
      '<div class="calc-display"><span class="calc-amount">' + (calc.amount ? h(calc.amount) : "0") + '</span><span class="calc-cur">' + currency() + "</span></div>" +
      addCategoryRow(calc, categories) +
      '<div class="calc-keys">' +
      keys.map(function (key) {
        var label = key === "back" ? "◀" : key;
        return '<button class="calc-key" type="button" data-act="add-key" data-key="' + key + '">' + label + "</button>";
      }).join("") +
      "</div>" +
      '<div class="field"><span class="field-label">Что купили</span>' +
      '<input class="input" id="addTitle" type="text" maxlength="60" placeholder="майонез" value="' + h(calc.title) + '" autocomplete="off"></div>' +
      (quick.length ? '<div class="chips" style="margin-bottom:14px">' + quick.map(function (title) {
        return '<button class="chip' + (calc.title === title ? " is-active" : "") + '" type="button" data-act="add-title" data-title="' + h(title) + '">' + h(title) + "</button>";
      }).join("") + "</div>" : "") +
      (calc.wishlistId ? "" :
        '<div class="field"><span class="field-label">Кто платил</span><div class="chips">' +
        members.map(function (member) {
          return '<button class="chip' + (calc.member === member.key ? " is-active" : "") + '" type="button" data-act="add-payer" data-member="' + h(member.key) + '" style="display:flex;align-items:center;gap:6px">' +
            art.avatar(member.key, { size: 22 }) + h(member.name) + "</button>";
        }).join("") + "</div></div>") +
      last;
  }

  function syncAdd() {
    var titleInput = el.panelRoot.querySelector("#addTitle");
    if (titleInput) { state.calc.title = titleInput.value; }
    var spec = state.panelSpec;
    var body = el.panelRoot.querySelector(".panel-body");
    if (body && spec && spec.body === addBody) {
      // Сохраняем позицию скролла: иначе панель прыгает наверх на каждом нажатии.
      var scrollTop = body.scrollTop;
      body.innerHTML = addBody();
      body.scrollTop = scrollTop;
      bindAddInput();
    }
  }

  function bindAddInput() {
    var input = el.panelRoot.querySelector("#addTitle");
    if (!input) { return; }
    input.addEventListener("input", function () {
      state.calc.title = input.value;
      var save = el.panelRoot.querySelector('[data-act="add-save"]');
      if (save) { save.disabled = !(calcValue() > 0 && state.calc.title.trim()); }
      if (!state.calc.categoryTouched) {
        // Пока категорию не выбирали руками — подставляем по истории названий.
        var guessed = guessCategory(input.value);
        if (guessed && guessed !== state.calc.category) {
          state.calc.category = guessed;
          refreshCategoryRow();
        }
      }
    });
    input.addEventListener("focus", function () {
      window.setTimeout(function () { input.scrollIntoView({ block: "center", behavior: "smooth" }); }, 250);
    });
  }

  function saveExpense() {
    var calc = state.calc;
    var title = (calc.title || "").trim();
    if (!title || calcValue() <= 0) { toast("Заполни сумму и название", { type: "error" }); return; }
    var request = calc.wishlistId
      ? api("/wishlist/" + encodeURIComponent(calc.wishlistId) + "/done", {
        method: "POST",
        body: { amount: calcValue(), category: calc.category }
      })
      : api("/expenses", {
        method: "POST",
        body: { amount: calcValue(), title: title, category: calc.category, member_id: calc.member }
      });
    request.then(function (payload) {
      sfx.print();
      haptic("success");
      var expense = payload.expense;
      showReceipt(expense);
      return refresh().then(function () {
        scene.animateCoin(el.stage);
        scene.hopMember(el.stage, expense.member_id);
        scene.hopCat(el.stage);
        window.setTimeout(function () { sfx.coin(); }, 300);
        window.setTimeout(function () {
          if (state.panelSpec && state.panelSpec.title === "ЧЕК") { renderResultPanel(expense); }
        }, 1700);
      });
    }).catch(fail);
  }

  function showReceipt(expense) {
    var jokes = ["майонез одобрен администрацией", "общага благодарит за вклад", "внесено в летопись соседей"];
    setPanel({
      title: "ЧЕК",
      body: function () {
        return '<div class="receipt">' +
          '<div class="receipt-title">СОСЕДИ • ЧЕК</div>' +
          '<div class="receipt-dash"></div>' +
          art.avatar(expense.member_id, { size: 44 }) +
          '<div class="receipt-item">' + h(expense.title) + "</div>" +
          '<div class="receipt-amount">' + money(expense.amount) + "\u00a0" + currency() + "</div>" +
          '<div class="receipt-dash"></div>' +
          '<div class="receipt-joke">' + h(jokes[expense.title.length % jokes.length]) + "</div>" +
          "</div>";
      }
    });
  }

  function renderResultPanel(expense) {
    setPanel({
      title: "ЗАПИСАНО",
      body: function () {
        var row = leaderRow(expense.member_id);
        return '<div class="center" style="padding:6px 0 14px">' +
          art.avatar(expense.member_id, { size: 56 }) +
          '<div class="row-title" style="margin-top:8px">' + h(expense.title) + "</div>" +
          '<div class="row-amount" style="font-size:12px">' + money(expense.amount) + "\u00a0" + currency() + "</div>" +
          '<p class="muted" style="margin-top:10px">' + h(memberName(expense.member_id)) + " теперь вложил " + money(row.total) + "\u00a0" + currency() + "</p>" +
          "</div>";
      },
      foot: function () {
        return '<div class="stack-gap">' +
          '<button class="btn primary block" type="button" data-act="add-open">ЗАПИСАТЬ ЕЩЁ</button>' +
          '<button class="btn ghost block" type="button" data-act="share-receipt" data-id="' + h(expense.id) + '">ПОДЕЛИТЬСЯ ЧЕКОМ</button>' +
          '<button class="btn ghost block" type="button" data-act="share-story" data-id="' + h(expense.id) + '">СТОРИС ВЕРТИКАЛЬНЫЙ</button>' +
          '<button class="btn ghost block" type="button" data-act="panel-close">ГОТОВО</button>' +
          "</div>";
      }
    });
  }

  // --- история ----------------------------------------------------------

  function openHistory() {
    state.historyRows = null;
    state.historyLimit = 20;
    setPanel({
      title: "ЧЕКИ",
      sub: money(state.data.totals_all.total) + "\u00a0" + currency(),
      body: historyBody,
      foot: function () {
        return '<button class="btn ghost block" type="button" data-act="export">ВЫГРУЗИТЬ CSV</button>';
      }
    });
    loadHistory();
  }

  function loadHistory() {
    var params = ["limit=" + state.historyLimit, "offset=0"];
    if (state.historyFilter) { params.push("member=" + encodeURIComponent(state.historyFilter)); }
    if (state.historyCategory) { params.push("category=" + encodeURIComponent(state.historyCategory)); }
    if (state.historyQuery) { params.push("q=" + encodeURIComponent(state.historyQuery)); }
    return api("/expenses?" + params.join("&")).then(function (payload) {
      state.historyRows = payload.expenses || [];
      state.historyTotal = payload.total || 0;
      state.historyHasMore = !!payload.has_more;
      drawPanel();
      return payload;
    }).catch(fail);
  }

  function historyFilters() {
    var categories = state.data.categories || [];
    var memberRow = '<div class="chips" style="margin-bottom:8px">' +
      '<button class="chip' + (!state.historyFilter ? " is-active" : "") + '" type="button" data-act="hist-filter" data-member="">Все</button>' +
      state.data.members.map(function (member) {
        return '<button class="chip' + (state.historyFilter === member.key ? " is-active" : "") + '" type="button" data-act="hist-filter" data-member="' + h(member.key) + '">' + h(member.name) + "</button>";
      }).join("") + "</div>";
    var categoryRow = '<div class="chips" style="margin-bottom:8px">' +
      '<button class="chip' + (!state.historyCategory ? " is-active" : "") + '" type="button" data-act="hist-cat" data-cat="">Все категории</button>' +
      categories.map(function (category) {
        return '<button class="chip' + (state.historyCategory === category.key ? " is-active" : "") + '" type="button" data-act="hist-cat" data-cat="' + h(category.key) + '">' + h(category.name) + "</button>";
      }).join("") + "</div>";
    var searchRow = '<div class="row-between" style="gap:8px;margin-bottom:12px">' +
      '<input class="input" id="histQuery" type="search" maxlength="40" placeholder="поиск: майонез" value="' + h(state.historyQuery) + '" autocomplete="off">' +
      '<button class="btn primary" type="button" data-act="hist-search">🔍</button></div>';
    return memberRow + categoryRow + searchRow;
  }

  function historyBody() {
    var filters = historyFilters();
    var rows = state.historyRows;
    if (rows === null) {
      return filters + '<div class="empty">считаю…</div>';
    }
    if (!rows.length) {
      return filters + '<div class="empty">Ничего не нашлось. Попробуй другой фильтр.</div>';
    }
    var body = filters + '<div class="rows">' + rows.map(function (row) {
      var mine = row.member_id === state.me || state.data.is_owner;
      var reactions = REACTIONS.map(function (reaction) {
        var owners = Object.keys(row.reactions || {}).filter(function (owner) { return row.reactions[owner] === reaction.key; });
        var active = row.reactions && row.reactions[state.me] === reaction.key;
        return '<button class="reaction' + (active ? " is-mine" : "") + '" type="button" data-act="exp-react" data-id="' + h(row.id) + '" data-r="' + reaction.key + '" aria-label="' + reaction.label + '">' +
          reaction.icon + (owners.length ? " " + owners.length : "") + "</button>";
      }).join("");
      return '<div class="row"><div class="row-avatar">' + art.avatar(row.member_id, { size: 30 }) + "</div>" +
        '<div class="row-main"><div class="row-title">' + h(row.title) + "</div>" +
        '<div class="row-sub">' + h(memberName(row.member_id)) + " • " + formatWhen(row.created_at) + " • " + h(categoryName(row.category)) + "</div>" +
        '<div class="reactions">' + reactions + "</div></div>" +
        '<div class="row-amount">' + money(row.amount) + "\u00a0" + currency() + "</div>" +
        (mine ? '<div class="row-actions"><button class="icon-mini" type="button" data-act="exp-del" data-id="' + h(row.id) + '" aria-label="Удалить">✕</button></div>' : "") +
        "</div>";
    }).join("") + "</div>";
    body += '<p class="center muted" style="margin-top:12px">показано ' + rows.length + " из " + state.historyTotal + "</p>";
    if (state.historyHasMore) {
      body += '<button class="btn ghost block" type="button" data-act="hist-more" style="margin-top:8px">ПОКАЗАТЬ ЕЩЁ</button>';
    }
    return body;
  }

  // --- списки: вишлист и запросы ---------------------------------------

  function openLists() {
    setPanel({ title: "СПИСКИ", body: listsBody });
  }

  function listsBody() {
    var tabs = '<div class="chips" style="margin-bottom:12px">' +
      '<button class="chip' + (state.listsTab === "wishlist" ? " is-active" : "") + '" type="button" data-act="lists-tab" data-tab="wishlist">Надо купить</button>' +
      '<button class="chip' + (state.listsTab === "requests" ? " is-active" : "") + '" type="button" data-act="lists-tab" data-tab="requests">Запросы</button>' +
      "</div>";
    return tabs + (state.listsTab === "wishlist" ? wishlistBody() : requestsBody());
  }

  function wishlistBody() {
    var open = state.data.wishlist_open || [];
    var done = (state.data.wishlist_done || []).slice(0, 6);
    var notes = state.data.notes || [];
    var addForm = '<div class="row-between" style="gap:8px;margin-bottom:12px">' +
      '<input class="input" id="wishTitle" type="text" maxlength="60" placeholder="что надо купить" autocomplete="off">' +
      '<button class="btn primary" type="button" data-act="wish-add">+</button></div>';
    var openHtml = open.length ? '<div class="rows">' + open.map(function (item) {
      var claimed = item.claimed_by;
      return '<div class="note-row' + (claimed ? " is-claimed" : "") + '">' +
        '<button class="note-check" type="button" data-act="wish-claim" data-id="' + h(item.id) + '" aria-label="Я возьму">' + (claimed ? "●" : "○") + "</button>" +
        '<div class="row-main"><div class="row-title">' + h(item.title) + "</div>" +
        '<div class="row-sub">' + (claimed ? h(memberName(claimed)) + " берёт" : "свободно") + "</div></div>" +
        '<div class="row-actions">' +
        '<button class="icon-mini" type="button" data-act="wish-done" data-id="' + h(item.id) + '" aria-label="Куплено">✓</button>' +
        '<button class="icon-mini" type="button" data-act="wish-del" data-id="' + h(item.id) + '" aria-label="Удалить">✕</button>' +
        "</div></div>";
    }).join("") + "</div>" : '<div class="empty">Список пуст. Добавьте, что кончилось.</div>';
    var doneHtml = done.length ? '<div class="section-title">УЖЕ КУПЛЕНО</div><div class="rows">' + done.map(function (item) {
      return '<div class="note-row is-done"><div class="row-main"><div class="row-title">' + h(item.title) + "</div>" +
        '<div class="row-sub">' + h(memberName(item.claimed_by)) + " • " + formatWhen(item.done_at) + "</div></div></div>";
    }).join("") + "</div>" : "";
    var notesHtml = '<div class="section-title">ЗАПИСКИ НА ХОЛОДИЛЬНИКЕ</div>' +
      '<div class="row-between" style="gap:8px;margin-bottom:10px">' +
      '<input class="input" id="noteText" type="text" maxlength="80" placeholder="кто съел моё?.." autocomplete="off">' +
      '<button class="btn primary" type="button" data-act="note-add">+</button></div>' +
      (notes.length ? '<div class="rows">' + notes.map(function (note) {
        return '<div class="note-row"><div class="row-main"><div class="row-title">' + h(note.text) + "</div>" +
          '<div class="row-sub">' + h(memberName(note.created_by)) + " • " + formatWhen(note.created_at) + "</div></div>" +
          '<div class="row-actions"><button class="icon-mini" type="button" data-act="note-del" data-id="' + h(note.id) + '" aria-label="Снять">✕</button></div></div>';
      }).join("") + "</div>" : '<div class="empty">Холодильник чистый.</div>');
    return addForm + openHtml + doneHtml + notesHtml;
  }

  function requestsBody() {
    var requests = state.data.requests || [];
    var members = state.data.members;
    var form = '<div class="field"><span class="field-label">Скинуться на что-то</span>' +
      '<input class="input" id="reqTitle" type="text" maxlength="60" placeholder="шампунь" autocomplete="off"></div>' +
      '<div class="field"><div class="chips">' +
      '<input class="input" id="reqAmount" type="text" inputmode="decimal" placeholder="150" style="max-width:120px">' +
      '<button class="chip is-active" type="button" data-act="req-toggle" data-per="1">по ' + h(members.length - 1) + " с каждого</button>" +
      "</div></div>" +
      '<div class="field"><span class="field-label">С кого</span><div class="chips" id="reqScope">' +
      '<button class="chip is-active" type="button" data-act="req-scope" data-scope="all">Со всех</button>' +
      members.filter(function (member) { return member.key !== state.me; }).map(function (member) {
        return '<button class="chip" type="button" data-act="req-scope" data-scope="' + h(member.key) + '">' + h(member.name) + "</button>";
      }).join("") + "</div></div>" +
      '<button class="btn primary block" type="button" data-act="req-create" style="margin-bottom:16px">СОЗДАТЬ ЗАПРОС</button>';
    var list = requests.length ? requests.map(function (item) {
      var progress = item.progress || {};
      var percent = progress.expected_total ? Math.min(100, Math.round(100 * progress.collected / progress.expected_total)) : 0;
      var paid = (progress.paid_by || []).indexOf(state.me) !== -1;
      var isAuthor = item.created_by === state.me;
      var scopeText = item.scope === "all" ? "со всех" : "с " + memberName(item.scope);
      return '<div class="row" style="display:block"><div class="row-between">' +
        "<div><div class=\"row-title\">" + h(item.title) + "</div>" +
        '<div class="row-sub">' + h(memberName(item.created_by)) + " просит " + money(item.amount) + "\u00a0" + currency() + " • " + h(scopeText) + "</div></div>" +
        '<div class="row-amount">' + money(progress.collected) + " / " + money(progress.expected_total) + "</div></div>" +
        '<div class="req-progress"><div style="width:' + percent + '%"></div></div>' +
        '<div class="row-between" style="margin-top:10px">' +
        '<span class="row-sub">' + progress.paid_count + " из " + progress.target_count + " скинулись</span>" +
        '<span class="row-actions">' +
        (paid ? "" : '<button class="btn ghost" type="button" data-act="req-pay" data-id="' + h(item.id) + '">СКИНУЛ</button>') +
        '<button class="btn ghost" type="button" data-act="req-poke" data-id="' + h(item.id) + '">ПНУТЬ</button>' +
        (isAuthor ? '<button class="btn danger" type="button" data-act="req-close" data-id="' + h(item.id) + '">ЗАКРЫТЬ</button>' : "") +
        "</span></div></div>";
    }).join("") : '<div class="empty">Запросов нет. Если что-то общее — создай запрос.</div>';
    return form + '<div class="section-title">АКТИВНЫЕ</div><div class="rows">' + list + "</div>";
  }

  // --- зал славы --------------------------------------------------------

  function openAchievements() {
    setPayloadPanel({ title: "ЗАЛ СЛАВЫ", body: achievementsBody });
  }

  function achievementsBody() {
    var badges = state.data.achievements || [];
    var progress = state.data.achievement_progress || [];
    var progressByMember = {};
    progress.forEach(function (row) {
      (progressByMember[row.member_id] = progressByMember[row.member_id] || []).push(row);
    });
    var hints = state.data.secret_hints || [];
    var members = state.data.members || [];
    var anyProgress = progress.filter(function (row) { return !row.done; });
    var sections = members.map(function (member) {
      var mine = badges.filter(function (badge) { return badge.member_id === member.key; });
      var pending = (progressByMember[member.key] || []).filter(function (row) { return !row.done; });
      if (!mine.length && !pending.length) { return ""; }
      var badgeHtml = mine.length ? '<div class="badges">' + mine.map(function (badge) {
        return '<div class="badge' + (badge.secret ? " is-secret" : "") + '">' + art.badge(badge.icon, { size: 26, color: member.color }) +
          '<div class="badge-title">' + h(badge.title) + "</div>" +
          (badge.detail ? '<div class="badge-detail">' + h(badge.detail) + "</div>" : "") +
          "</div>";
      }).join("") + "</div>" : '<div class="empty">кубков пока нет</div>';
      var progressHtml = pending.length ? '<div class="progress-list">' + pending.slice(0, 5).map(function (row) {
        var label = progressLabel(row.id);
        var percent = row.target > 0 ? Math.min(100, Math.round(100 * row.progress / row.target)) : 0;
        var value = row.target >= 1000
          ? money(row.progress) + " / " + money(row.target) + "\u00a0" + currency()
          : row.progress + " / " + row.target;
        return '<div class="progress-row"><div class="pie-head"><span>' + h(label) + "</span><span>" + h(value) + "</span></div>" +
          '<div class="req-progress"><div style="width:' + percent + '%"></div></div></div>';
      }).join("") + "</div>" : "";
      return '<div class="section-title">' + h(memberName(member.key)).toUpperCase() + "</div>" + badgeHtml + progressHtml;
    }).join("");
    var hintHtml = hints.length ? '<div class="section-title">СЕКРЕТНЫЕ КУБКИ</div><div class="secret-grid">' +
      hints.map(function (hint) {
        var unlocked = badges.some(function (badge) { return badge.id.indexOf("secret_") === 0; });
        return '<div class="secret-card"><div class="secret-mark">?</div>' +
          '<div class="badge-title">' + h(unlocked ? hint.title : "？？？") + "</div>" +
          '<div class="badge-detail">' + h(unlocked ? hint.title : "условие скрыто") + "</div></div>";
      }).join("") + "</div>" : "";
    if (!badges.length && !anyProgress.length) {
      return '<div class="empty">Кубков пока нет. Первая покупка всё начнёт.</div>' + hintHtml;
    }
    return sections + hintHtml;
  }

  function progressLabel(id) {
    var labels = {
      collector: "разных категорий",
      repeat: "повторов одного товара",
      patron: "покупок за месяц",
      stability: "недель подряд",
      veteran: "дней с первого вклада",
      big_one: "крупнейшая покупка",
      secret_marathon: "дней подряд",
      secret_sugar_daddy: "вложено за месяц"
    };
    return labels[id] || id;
  }

  // --- цифры: бюджет, средний чек, пирог, прогноз -----------------------

  function openStats() {
    setPanel({ title: "ЦИФРЫ СОСЕДЕЙ", body: statsBody });
  }

  function statsBody() {
    var data = state.data;
    var budget = data.budget || {};
    var forecast = data.forecast || {};
    var spending = data.spending || {};
    var categories = spending.categories || [];
    var lines = [];
    if (budget.enabled) {
      var percent = Math.min(100, Math.round((budget.progress || 0) * 100));
      var over = (budget.progress || 0) >= 1;
      lines.push('<div class="stat-hero' + (over ? " is-over" : "") + '">' +
        '<div class="row-sub">бюджет месяца</div>' +
        '<div class="row-amount" style="font-size:18px">' + money(budget.spent) + " / " + money(budget.limit) + "\u00a0" + currency() + "</div>" +
        '<div class="req-progress" style="margin-top:10px"><div style="width:' + percent + '%"></div></div>' +
        '<div class="row-sub" style="margin-top:8px">' +
        (over
          ? "перебор на " + money(budget.over) + "\u00a0" + currency()
          : "осталось " + money(budget.remaining) + "\u00a0" + currency() +
            (budget.daily_allowance ? " • можно тратить ~" + money(budget.daily_allowance) + " в день" : "")) +
        "</div></div>");
      if (forecast.will_exceed) {
        lines.push('<p class="muted center" style="margin:8px 0 0">темп ведёт к ' + money(forecast.projected) + "\u00a0" + currency() + " к концу месяца — это больше лимита</p>");
      } else if (forecast.projected) {
        lines.push('<p class="muted center" style="margin:8px 0 0">по темпу выйдет ~' + money(forecast.projected) + "\u00a0" + currency() + " за месяц</p>");
      }
    }
    var rows = '<div class="rows" style="margin-top:12px">' +
      '<div class="row"><div class="row-main"><div class="row-sub">Средний чек</div></div><div class="row-amount">' + money(spending.average) + "\u00a0" + currency() + "</div></div>" +
      '<div class="row"><div class="row-main"><div class="row-sub">Покупок за период</div></div><div class="row-amount">' + (spending.count || 0) + "</div></div>" +
      (spending.biggest ? '<div class="row"><div class="row-main"><div class="row-sub">Крупнейшая</div><div class="row-title">' + h(spending.biggest.title) + '</div></div><div class="row-amount">' + money(spending.biggest.amount) + "\u00a0" + currency() + "</div></div>" : "") +
      "</div>";
    var pie = categories.length ? '<div class="section-title">КУДА УХОДЯТ ДЕНЬГИ</div><div class="pie-list">' +
      categories.map(function (row) {
        var percent = Math.round((row.share || 0) * 100);
        return '<div class="pie-row"><div class="pie-head"><span>' + h(row.name) + "</span><span>" + money(row.total) + "\u00a0" + currency() + " • " + percent + "%</span></div>" +
          '<div class="req-progress"><div style="width:' + Math.max(2, percent) + '%"></div></div></div>';
      }).join("") + "</div>" : "";
    var footer = '<div class="stack-gap" style="margin-top:18px">' +
      '<button class="btn ghost block" type="button" data-act="share-month">СПОНСОР МЕСЯЦА КАРТИНКОЙ</button>' +
      '<button class="btn ghost block" type="button" data-act="calendar-open">КАЛЕНДАРЬ АКТИВНОСТИ</button>' +
      "</div>";
    return (lines.length ? lines.join("") : '<div class="empty">Бюджет не задан — поставь monthly_budget в config/obshak.yaml.</div>') + rows + pie + footer;
  }

  // --- карточка участника ----------------------------------------------

  function openMember(memberKey) {
    setPanel({
      title: memberName(memberKey).toUpperCase(),
      body: function () { return '<div class="center muted">считаю…</div>'; }
    });
    api("/members/" + encodeURIComponent(memberKey)).then(function (payload) {
      setPanel({
        title: (payload.member && payload.member.name || "УЧАСТНИК").toUpperCase(),
        body: function () { return memberBody(payload); },
        foot: function () {
          return '<button class="btn ghost block" type="button" data-act="share-cert" data-member="' + h(memberKey) + '">ГРАМОТА УЧАСТНИКА</button>';
        }
      });
    }).catch(fail);
  }

  function memberBody(payload) {
    var stats = payload.stats || {};
    var badges = payload.badges || [];
    var biggest = stats.biggest;
    return "" +
      '<div class="center" style="padding-bottom:12px">' + art.avatar(payload.member.key, { size: 72 }) +
      '<div class="section-title" style="margin-top:10px">МЕСТО ' + (payload.rank || "—") + " ИЗ " + payload.members_total + "</div></div>" +
      '<div class="rows" style="margin-bottom:14px">' +
      '<div class="row"><div class="row-main"><div class="row-sub">Вложено всего</div></div><div class="row-amount">' + money(stats.total) + "\u00a0" + currency() + "</div></div>" +
      '<div class="row"><div class="row-main"><div class="row-sub">Покупок</div></div><div class="row-amount">' + (stats.count || 0) + "</div></div>" +
      '<div class="row"><div class="row-main"><div class="row-sub">Средний чек</div></div><div class="row-amount">' + money(stats.average) + "\u00a0" + currency() + "</div></div>" +
      (stats.top_category ? '<div class="row"><div class="row-main"><div class="row-sub">Любимая категория</div></div><div class="row-amount">' + h(categoryName(stats.top_category.key)) + "</div></div>" : "") +
      (biggest ? '<div class="row"><div class="row-main"><div class="row-sub">Крупнейшая покупка</div><div class="row-title">' + h(biggest.title) + "</div></div><div class=\"row-amount\">" + money(biggest.amount) + "\u00a0" + currency() + "</div></div>" : "") +
      "</div>" +
      (badges.length ? '<div class="section-title">КУБКИ</div><div class="badges">' + badges.map(function (badge) {
        return '<div class="badge">' + art.badge(badge.icon, { size: 26, color: payload.member.color }) +
          '<div class="badge-title">' + h(badge.title) + "</div></div>";
      }).join("") + "</div>" : "") +
      '<div class="section-title">ПОСЛЕДНИЕ ПОКУПКИ</div>' +
      ((stats.recent || []).length ? '<div class="rows">' + stats.recent.map(function (row) {
        return '<div class="row"><div class="row-main"><div class="row-title">' + h(row.title) + "</div>" +
          '<div class="row-sub">' + formatWhen(row.created_at) + "</div></div>" +
          '<div class="row-amount">' + money(row.amount) + "</div></div>";
      }).join("") + "</div>" : '<div class="empty">Покупок ещё нет.</div>');
  }

  // --- рейтинг и гонка --------------------------------------------------

  function openRating() {
    setPanel({ title: "РЕЙТИНГ", sub: periodLabel(), body: ratingBody });
  }

  function periodLabel() {
    return state.period === "week" ? "неделя" : state.period === "month" ? "месяц" : "всё время";
  }

  function ratingBody() {
    var rows = state.data.leaderboard || [];
    var max = rows.reduce(function (acc, row) { return Math.max(acc, row.total); }, 0);
    var bars = '<div class="bars">' + rows.map(function (row) {
      var percent = max > 0 ? Math.max(3, Math.round(100 * row.total / max)) : 3;
      return '<div class="bar"><span class="bar-value">' + money(row.total) + "</span>" +
        '<div class="bar-track"><div class="bar-fill" style="height:' + percent + "%;background:" + h(row.color) + '"></div></div>' +
        '<span class="bar-name">' + h(row.name) + "</span></div>";
    }).join("") + "</div>";
    var race = state.data.race || { lanes: [] };
    var lanes = '<div class="rows">' + (race.lanes || []).map(function (lane) {
      var percent = Math.round((lane.position || 0) * 100);
      return '<div class="row" style="display:block"><div class="row-between">' +
        '<div class="row-title">' + h(lane.name) + "</div>" +
        '<div class="row-amount">' + money(lane.total) + "\u00a0" + currency() + "</div></div>" +
        '<div class="req-progress"><div style="width:' + percent + "%;background:" + h(lane.color) + '"></div></div></div>';
    }).join("") + "</div>";
    return '<div class="section-title">ВКЛАД ЗА ' + h(periodLabel().toUpperCase()) + "</div>" + bars +
      '<div class="section-title">ГОНКА НЕДЕЛИ</div>' + lanes +
      '<p class="muted center" style="margin-top:10px">Сброс через ' + countdown(race.seconds_to_reset) + "</p>";
  }

  function countdown(seconds) {
    seconds = Math.max(0, Number(seconds) || 0);
    var days = Math.floor(seconds / 86400);
    var hours = Math.floor((seconds % 86400) / 3600);
    var minutes = Math.floor((seconds % 3600) / 60);
    if (days > 0) { return days + " д " + hours + " ч"; }
    if (hours > 0) { return hours + " ч " + minutes + " мин"; }
    return minutes + " мин";
  }

  // --- банка, кот, новости, календарь ----------------------------------

  function setPayloadPanel(spec) {
    setPanel(spec);
  }

  function openJar() {
    setPanel({ title: "ОБЩАЯ БАНКА", body: jarBody });
  }

  function jarBody() {
    var jar = state.data.jar;
    var percent = Math.round((jar.progress || 0) * 100);
    var levels = (jar.levels || []).map(function (level, index) {
      var reached = index <= jar.level.index;
      return '<div class="row" style="opacity:' + (reached ? 1 : 0.45) + '">' +
        '<div class="row-main"><div class="row-title">' + h(level.name) + "</div>" +
        '<div class="row-sub">от ' + money(level.threshold) + "\u00a0" + currency() + "</div></div>" +
        '<div class="row-amount">' + (reached ? "✓" : "—") + "</div></div>";
    }).join("");
    return '<div class="center" style="padding-bottom:12px">' +
      '<div class="row-amount" style="font-size:20px">' + money(jar.total) + "\u00a0" + currency() + "</div>" +
      '<div class="section-title">' + h(jar.level.name) + "</div></div>" +
      '<div class="req-progress" style="height:16px"><div style="width:' + percent + '%"></div></div>' +
      '<p class="center muted" style="margin-top:10px">' +
      (jar.next ? "До «" + h(jar.next.name) + "» осталось " + money(jar.remaining) + "\u00a0" + currency() : "Максимальный уровень. Банка трещит.") +
      "</p>" +
      '<div class="section-title">УРОВНИ</div><div class="rows">' + levels + "</div>";
  }

  function openPet() {
    setPanel({ title: "КОТ " + (state.data.pet.name || "").toUpperCase(), body: petBody });
  }

  function petBody() {
    var pet = state.data.pet;
    var xpToNext = pet.next ? Math.max(0, pet.next.threshold - (pet.xp || pet.count)) : 0;
    var xpFrom = pet.level.threshold || 0;
    var xpSpan = pet.next ? Math.max(1, pet.next.threshold - xpFrom) : 1;
    var xpPercent = pet.next ? Math.min(100, Math.round(100 * ((pet.xp || pet.count) - xpFrom) / xpSpan)) : 100;
    return '<div class="center" style="padding-bottom:14px">' +
      '<div class="pet-speech">' + h(pet.phrase) + "</div>" +
      art.cat(pet.sleeping, { size: 96 }) +
      "</div>" +
      '<div class="rows">' +
      '<div class="row"><div class="row-main"><div class="row-sub">Уровень</div><div class="row-title">' + h(pet.level.name) + "</div></div></div>" +
      '<div class="row"><div class="row-main"><div class="row-sub">Настроение</div><div class="row-title">' + h(moodLabel(pet.mood)) + "</div></div></div>" +
      '<div class="row"><div class="row-main"><div class="row-sub">Опыт</div><div class="row-title">' + (pet.xp || pet.count) + " XP</div></div><div class=\"row-amount\">" + (pet.pats ? pet.pats + " мур" : "") + "</div></div>" +
      '<div class="row"><div class="row-main"><div class="row-sub">Съедено покупок</div></div><div class="row-amount">' + pet.count + "</div></div>" +
      (pet.next ? '<div class="row"><div class="row-main"><div class="row-sub">До уровня «' + h(pet.next.name) + '»</div></div><div class="row-amount">' + xpToNext + " XP</div></div>" : "") +
      (pet.days_since_last !== null && pet.days_since_last !== undefined ? '<div class="row"><div class="row-main"><div class="row-sub">Дней без покупок</div></div><div class="row-amount">' + pet.days_since_last + "</div></div>" : "") +
      "</div>" +
      (pet.next ? '<div class="req-progress" style="margin-top:12px"><div style="width:' + xpPercent + '%"></div></div>' : "") +
      '<p class="muted center" style="margin-top:10px">XP растёт от покупок, серии дней и закрытых квестов.</p>' +
      '<button class="btn ghost block" type="button" data-act="pet-pat" style="margin-top:10px">ПОГЛАДИТЬ</button>';
  }

  function moodLabel(mood) {
    return { happy: "довольный", proud: "гордый", bored: "скучает", hungry: "голодный" }[mood] || mood;
  }

  function openNews() {
    setPanel({ title: "СОСЕДСКИЕ НОВОСТИ", body: newsBody });
  }

  function newsBody() {
    var headlines = state.data.news || [];
    var today = new Date();
    var months = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля", "августа", "сентября", "октября", "ноября", "декабря"];
    return '<div class="news-item"><div class="news-kicker">ВЫПУСК • ' + today.getDate() + " " + months[today.getMonth()] + "</div>" +
      '<div class="news-text">СОСЕДИ ЖИВЫ</div></div>' +
      headlines.map(function (headline) {
        return '<div class="news-item"><div class="news-text">' + h(headline.text) + "</div></div>";
      }).join("");
  }

  function openCalendar() {
    setPayloadPanel({ title: "КАЛЕНДАРЬ", body: calendarBody });
  }

  function calendarBody() {
    var cal = state.data.calendar;
    if (!cal) {
      return '<div class="center muted">считаю…</div>';
    }
    var days = cal.days || [];
    var peak = cal.peak || 0;
    var cells = days.map(function (day) {
      var intensity = peak > 0 ? day.total / peak : 0;
      var level = day.count === 0 ? 0 : Math.max(1, Math.ceil(intensity * 4));
      var color = ["var(--panel-2)", "#2f5a44", "#3f9a6a", "#63c491", "#ffd447"][level];
      return '<div class="heat-cell" style="background:' + color + '" data-act="cal-day" data-day="' + h(day.date) + '" title="' + h(day.date) + ": " + money(day.total) + '"></div>';
    }).join("");
    var filters = '<div class="chips" style="margin-bottom:12px">' +
      '<button class="chip' + (!state.calendarMember ? " is-active" : "") + '" type="button" data-act="cal-member" data-member="">Все</button>' +
      state.data.members.map(function (member) {
        return '<button class="chip' + (state.calendarMember === member.key ? " is-active" : "") + '" type="button" data-act="cal-member" data-member="' + h(member.key) + '">' + h(member.name) + "</button>";
      }).join("") + "</div>";
    return filters + '<div class="heat-grid">' + cells + "</div>" +
      '<div class="heat-legend">меньше <span class="heat-cell" style="background:#2f5a44"></span><span class="heat-cell" style="background:#63c491"></span><span class="heat-cell" style="background:#ffd447"></span> больше</div>' +
      '<div class="section-title">ЗА ДЕНЬ</div><div id="calDayList" class="rows"><div class="empty">Тапни по клетке</div></div>';
  }

  function loadCalendar() {
    api("/calendar" + (state.calendarMember ? "?member=" + encodeURIComponent(state.calendarMember) : ""))
      .then(function (payload) {
        state.data.calendar = payload.calendar;
        drawPanel();
      }).catch(fail);
  }

  function openCalendarDay(day) {
    api("/calendar/" + encodeURIComponent(day)).then(function (payload) {
      var list = document.getElementById("calDayList");
      if (!list) { return; }
      var rows = payload.expenses || [];
      list.innerHTML = rows.length ? rows.map(function (row) {
        return '<div class="row"><div class="row-avatar">' + art.avatar(row.member_id, { size: 30 }) + "</div>" +
          '<div class="row-main"><div class="row-title">' + h(row.title) + "</div>" +
          '<div class="row-sub">' + h(memberName(row.member_id)) + " • " + formatWhen(row.created_at) + "</div></div>" +
          '<div class="row-amount">' + money(row.amount) + "</div></div>";
      }).join("") : '<div class="empty">В этот день ничего не купили.</div>';
      list.scrollIntoView({ block: "nearest", behavior: "smooth" });
    }).catch(fail);
  }

  function openHelp() {
    setPanel({ title: "ЧТО ЭТО", body: function () {
      return '<p style="line-height:1.5">Это касса соседей: кто что купил в общий котёл — записываем сюда, а кухня сама считает, кто сколько вложил.</p>' +
        '<div class="section-title">КАК ПОЛЬЗОВАТЬСЯ</div>' +
        '<div class="rows">' +
        '<div class="row"><div class="row-main"><div class="row-title">Калькулятор</div><div class="row-sub">записать покупку: сумма, что купил, кто платил</div></div></div>' +
        '<div class="row"><div class="row-main"><div class="row-title">Холодильник</div><div class="row-sub">список «надо купить»</div></div></div>' +
        '<div class="row"><div class="row-main"><div class="row-title">Доска с чеками</div><div class="row-sub">вся история покупок</div></div></div>' +
        '<div class="row"><div class="row-main"><div class="row-title">Банка</div><div class="row-sub">уровни общего котла</div></div></div>' +
        '<div class="row"><div class="row-main"><div class="row-title">Кот</div><div class="row-sub">живёт под столом и следит за активностью</div></div></div>' +
        "</div>" +
        '<div class="section-title">ЗАПРОСЫ ДЕНЕГ</div>' +
        '<p class="muted" style="line-height:1.5">По умолчанию никто никому не должен: соседи просто вкладываются в общее. Если надо скинуться — создай запрос, и он появится у всех.</p>';
    } });
  }

  // --- рулетка «кто идёт в магазин» ------------------------------------

  function openRoulette() {
    if (!state.roulette) { state.roulette = { spinning: false, winner: null }; }
    setPanel({
      title: "КТО ИДЁТ В МАГАЗИН",
      body: rouletteBody,
      foot: function () {
        var limits = (state.data.roulette && state.data.roulette.limits) || {};
        var spinning = state.roulette && state.roulette.spinning;
        var canSpin = limits.can_spin !== false && !spinning;
        var buttons = '<button class="btn primary block" type="button" data-act="roulette-spin"' +
          (canSpin ? "" : " disabled") + ">КРУТИТЬ</button>";
        if (state.roulette && state.roulette.winner && !spinning) {
          buttons += '<button class="btn ghost block" type="button" data-act="roulette-confirm" style="margin-top:8px">Я СХОДИЛ ✓</button>';
        }
        return buttons;
      }
    });
  }

  function slotFaceHtml(memberKey, spinning) {
    var member = memberByKey(memberKey);
    var color = member ? member.color : "#8b90a3";
    return '<div class="slot-face ' + (spinning ? "is-spin" : "is-win") + '">' +
      art.avatar(memberKey, { size: spinning ? 54 : 76 }) +
      '<span class="slot-name" style="color:' + h(color) + '">' + h(memberName(memberKey)) + "</span></div>";
  }

  function rouletteBody() {
    var data = state.data.roulette || { stats: { counts: {}, total: 0, last: null } };
    var limits = data.limits || {};
    var counts = data.stats.counts || {};
    var members = state.data.members;
    var weights = members.map(function (member) { return 1 / (1 + (counts[member.key] || 0)); });
    var totalWeight = weights.reduce(function (acc, value) { return acc + value; }, 0) || 1;
    var winner = state.roulette && state.roulette.winner;
    var spinning = state.roulette && state.roulette.spinning;
    var faces = members.map(function (member) { return member.key; });
    var winnerKey = winner || faces[0];

    var rows = members.map(function (member, index) {
      var count = counts[member.key] || 0;
      var chance = Math.round(100 * weights[index] / totalWeight);
      return '<div class="row"><div class="row-avatar">' + art.avatar(member.key, { size: 30 }) + "</div>" +
        '<div class="row-main"><div class="row-title">' + h(member.name) + "</div>" +
        '<div class="row-sub">ходил ' + count + " " + pluralTimes(count) + "</div></div>" +
        '<div class="row-amount">' + chance + "%</div></div>";
    }).join("");

    var last = data.stats.last;
    var lastLine = last && last.member_id
      ? '<p class="center muted" style="margin-top:10px">прошлый раз ходил ' + h(memberName(last.member_id)) + "</p>"
      : "";

    var cooldown = limits.cooldown_left || 0;
    var spinHint = "";
    if (cooldown > 0) {
      spinHint = '<p class="muted center" style="margin-top:8px">следующий бросок через ' + formatLeft(cooldown) + "</p>";
    } else if (limits.spins_left === 0) {
      spinHint = '<p class="muted center" style="margin-top:8px">на сегодня прокрутки кончились</p>';
    }

    var history = data.history || [];
    var historyHtml = history.length ? '<div class="section-title">ПОХОДЫ</div><div class="rows">' + history.slice(0, 8).map(function (entry) {
      return '<div class="row"><div class="row-avatar">' + art.avatar(entry.member_id, { size: 26 }) + "</div>" +
        '<div class="row-main"><div class="row-title">' + h(entry.name) + "</div>" +
        '<div class="row-sub">' + formatWhen(entry.ts) + (entry.confirmed_at ? " • сходил" : "") + "</div></div></div>";
    }).join("") + "</div>" : "";

    return '<div class="slot">' +
      '<div class="slot-reel" id="slotReel">' + slotFaceHtml(winnerKey, !!spinning) + "</div>" +
      '<div class="slot-caption" id="slotCaption">' +
      (winner ? h(memberName(winner)) + " идёт в магазин" : "нажми «крутить» — рулетка выберет честно") +
      "</div></div>" +
      spinHint +
      '<div class="section-title">ШАНСЫ</div><div class="rows">' + rows + "</div>" + lastLine +
      '<p class="muted" style="margin-top:14px;line-height:1.5">Рулетка подравнивает: кто ходил чаще, у того шанс ниже. ' +
      "За две недели: " + (data.stats.total || 0) + " " + pluralTimes(data.stats.total || 0) + ".</p>" +
      historyHtml +
      '<div class="panel-inline-foot">' +
      '<button class="btn ghost block" type="button" data-act="share-roulette">КАРТИНКОЙ В БЕСЕДУ</button>' +
      "</div>";
  }

  function formatLeft(seconds) {
    var value = Math.max(0, Math.round(seconds));
    if (value < 60) { return value + " сек"; }
    var minutes = Math.round(value / 60);
    if (minutes < 60) { return minutes + " мин"; }
    return Math.round(minutes / 60) + " ч";
  }

  function spinRoulette() {
    if (state.roulette && state.roulette.spinning) { return; }
    state.roulette = { spinning: true, winner: null };
    drawPanel();
    var timer = window.setInterval(function () {
      var node = document.getElementById("slotReel");
      if (!node) { return; }
      var random = state.data.members[Math.floor(Math.random() * state.data.members.length)];
      node.innerHTML = slotFaceHtml(random.key, true);
    }, 90);
    sfx.click();
    var started = Date.now();

    api("/roulette", { method: "POST", body: {} }).then(function (payload) {
      var wait = Math.max(0, 1500 - (Date.now() - started));
      window.setTimeout(function () {
        window.clearInterval(timer);
        state.roulette = { spinning: false, winner: payload.winner };
        var node = document.getElementById("slotReel");
        if (node) { node.innerHTML = slotFaceHtml(payload.winner, false); }
        var label = document.getElementById("slotCaption");
        if (label) { label.textContent = memberName(payload.winner) + " идёт в магазин"; }
        sfx.levelUp();
        haptic("success");
        scene.hopMember(el.stage, payload.winner);
        toast(memberName(payload.winner) + ", ты сегодня в магазин");
        refresh().catch(fail);
      }, wait);
    }).catch(function (error) {
      window.clearInterval(timer);
      state.roulette = { spinning: false, winner: null };
      fail(error);
    });
  }

  function confirmRoulette() {
    api("/roulette/confirm", { method: "POST" }).then(function (payload) {
      sfx.coin();
      haptic("success");
      toast("Поход засчитан: всего " + payload.total_confirmed);
      return refresh();
    }).catch(fail);
  }

  // --- стрик и квест недели ---------------------------------------------

  function openQuests() {
    setPanel({ title: "СТРИК И КВЕСТ", body: questsBody });
  }

  function questsBody() {
    var streak = state.data.streak || { current: 0, best: 0, days: [] };
    var quest = state.data.quest;
    var hot = streak.current > 0;
    var cells = (streak.days || []).map(function (day) {
      return '<span class="flame-cell" title="' + h(day.date) + '">' +
        art.badge("flame", { size: 16, color: day.hit ? "#ffd447" : "#3a3f52", contrast: "#2a2205" }) + "</span>";
    }).join("");

    var questHtml = '<div class="empty">На эту неделю квест не задан.</div>';
    if (quest) {
      var percent = quest.target > 0 ? Math.min(100, Math.round(100 * quest.progress / quest.target)) : 0;
      questHtml = '<div class="quest-card' + (quest.done ? " is-done" : "") + '">' +
        '<div class="quest-title">' + h(quest.title) + "</div>" +
        '<div class="row-sub" style="margin-top:6px">' + h(questProgressText(quest)) + "</div>" +
        '<div class="req-progress" style="margin-top:10px"><div style="width:' + percent + '%"></div></div>' +
        (quest.done ? '<div class="quest-stamp">ВЫПОЛНЕНО</div>' : "") +
        "</div>";
    }

    return '<div class="center" style="padding-bottom:12px">' +
      art.badge("flame", { size: 44, color: hot ? "#ffd447" : "#4c5165", contrast: "#2a2205" }) +
      '<div class="row-amount" style="font-size:18px;margin-top:10px">' +
      streak.current + " " + pluralDays(streak.current) + "</div>" +
      '<div class="row-sub">рекорд: ' + streak.best + " " + pluralDays(streak.best) + "</div>" +
      "</div>" +
      '<div class="flame-row">' + cells + "</div>" +
      '<div class="section-title">КВЕСТ НЕДЕЛИ</div>' + questHtml +
      '<button class="btn ghost block" type="button" data-act="share-streak" style="margin-top:14px">ОГОНЬ КАРТИНКОЙ</button>' +
      '<p class="muted" style="margin-top:14px;line-height:1.5">Стрик — дни подряд, когда соседи что-то покупали. ' +
      "Пропустили день — огонь гаснет. Квест общий и меняется каждый понедельник.</p>";
  }

  // --- кот при входе -----------------------------------------------------

  function showCatDialog() {
    var roast = state.data && state.data.roast;
    if (!roast || !roast.text) { return; }
    // Не перекрываем панель, если человек уже что-то открыл.
    if (state.panelSpec) { return; }
    if (shouldNeighborSpeak() && showNeighborDialog(roast.text)) { return; }
    closeCatDialog();
    var node = document.createElement("div");
    node.className = "cat-dialog";
    node.dataset.act = "cat-dialog-close";
    node.innerHTML = '<div class="cat-dialog-card">' +
      '<div class="cat-dialog-head">' + h((state.data.pet && state.data.pet.name) || "КОТ") + " ГОВОРИТ</div>" +
      '<div class="cat-dialog-body">' +
      art.cat(state.data.pet && state.data.pet.sleeping, { size: 68 }) +
      '<div class="cat-bubble">' + h(roast.text) + "</div></div>" +
      '<button class="btn primary block" type="button" data-act="cat-dialog-close">МУР</button>' +
      "</div>";
    el.catRoot.appendChild(node);
    scene.hopCat(el.stage);
    sfx.hop();
    state.catTimer = window.setTimeout(closeCatDialog, 9000);
  }

  function shouldNeighborSpeak() {
    // Примерно каждый второй вход реплику доставляет сосед, а не кот.
    return neighborSpeaker() !== null && Math.random() < 0.5;
  }

  function neighborSpeaker() {
    var members = (state.data && state.data.members) || [];
    var away = scene.awayLabels || {};
    var candidates = members.filter(function (member) {
      return member.key !== state.me && !away[member.key];
    });
    if (!candidates.length) { return null; }
    return candidates[Math.floor(Math.random() * candidates.length)];
  }

  function showNeighborDialog(text) {
    var speaker = neighborSpeaker();
    if (!speaker || !el.stage) { return false; }
    var portrait = el.stage.querySelector('.character[data-member="' + speaker.key + '"]');
    if (!portrait) { return false; }
    var stageRect = el.stage.getBoundingClientRect();
    var headRect = portrait.getBoundingClientRect();
    if (!stageRect.width || !headRect.width) { return false; }
    closeCatDialog();
    var bubble = document.createElement("button");
    bubble.type = "button";
    bubble.className = "scene-bubble";
    bubble.dataset.act = "cat-dialog-close";
    bubble.textContent = text;
    el.stage.appendChild(bubble);
    // Центр по голове персонажа; если сверху не влезает — разворачиваем вниз.
    var centerX = headRect.left - stageRect.left + headRect.width / 2;
    var headTop = headRect.top - stageRect.top;
    var bubbleRect = bubble.getBoundingClientRect();
    var above = headTop - bubbleRect.height - 12;
    if (above >= 0) {
      bubble.style.left = centerX + "px";
      bubble.style.top = headTop - 10 + "px";
      bubble.style.transform = "translate(-50%, -100%)";
    } else {
      bubble.classList.add("is-below");
      bubble.style.left = centerX + "px";
      bubble.style.top = headRect.bottom - stageRect.top + 8 + "px";
      bubble.style.transform = "translate(-50%, 0)";
    }
    sfx.hop();
    scene.hopMember(el.stage, speaker.key);
    haptic("light");
    state.catTimer = window.setTimeout(closeCatDialog, 8000);
    return true;
  }

  function closeCatDialog() {
    if (state.catTimer) { window.clearTimeout(state.catTimer); state.catTimer = null; }
    if (el.catRoot) { el.catRoot.innerHTML = ""; }
    var bubbles = el.stage ? el.stage.querySelectorAll(".scene-bubble") : [];
    Array.prototype.forEach.call(bubbles, function (node) { node.remove(); });
  }

  // --- делегирование событий -------------------------------------------

  function onAction(act, node) {
    switch (act) {
      case "panel-close": closePanel(); break;
      case "add-open": sfx.click(); openAdd(); break;
      case "history-open": sfx.click(); openHistory(); break;
      case "lists-open": sfx.click(); openLists(); break;
      case "wishlist-open": sfx.click(); state.listsTab = "wishlist"; openLists(); break;
      case "achievements-open": sfx.click(); openAchievements(); break;
      case "rating-open": sfx.click(); openRating(); break;
      case "jar-open": sfx.click(); openJar(); break;
      case "pet-open": sfx.click(); openPet(); break;
      case "news-open": sfx.click(); openNews(); break;
      case "calendar-open": sfx.click(); openCalendar(); loadCalendar(); break;
      case "help-open": sfx.click(); openHelp(); break;
      case "member-open": sfx.click(); openMember(node.dataset.member); break;

      case "add-key": addKey(node.dataset.key); break;
      case "add-title": syncAdd(); state.calc.title = node.dataset.title; syncAdd(); sfx.click(); break;
      case "add-cat":
        syncAdd();
        state.calc.category = node.dataset.cat;
        state.calc.categoryTouched = true;
        syncAdd();
        sfx.click();
        break;
      case "add-payer": syncAdd(); state.calc.member = node.dataset.member; syncAdd(); sfx.click(); break;
      case "add-repeat":
        if (state.calc.last) {
          state.calc.title = state.calc.last.title;
          state.calc.amount = String(state.calc.last.amount);
          state.calc.category = state.calc.last.category;
          syncAdd(); sfx.click();
        }
        break;
      case "add-save": syncAdd(); saveExpense(); break;

      case "hist-filter":
        state.historyFilter = node.dataset.member || null;
        state.historyLimit = 20;
        sfx.click();
        loadHistory();
        break;
      case "hist-cat":
        state.historyCategory = node.dataset.cat || null;
        state.historyLimit = 20;
        sfx.click();
        loadHistory();
        break;
      case "hist-search": {
        var queryNode = document.getElementById("histQuery");
        state.historyQuery = queryNode ? queryNode.value.trim() : "";
        state.historyLimit = 20;
        sfx.click();
        loadHistory();
        break;
      }
      case "hist-more":
        state.historyLimit += 20;
        sfx.click();
        loadHistory();
        break;
      case "exp-del":
        api("/expenses/" + encodeURIComponent(node.dataset.id), { method: "DELETE" })
          .then(function () { sfx.pop(); return refresh(); })
          .then(function () { return loadHistory(); })
          .catch(fail);
        break;
      case "exp-react":
        api("/expenses/" + encodeURIComponent(node.dataset.id) + "/reaction", { method: "POST", body: { reaction: node.dataset.r } })
          .then(function () { sfx.pop(); haptic("light"); return refresh(); })
          .then(function () { return loadHistory(); })
          .catch(fail);
        break;

      case "lists-tab": state.listsTab = node.dataset.tab; sfx.click(); drawPanel(); break;
      case "wish-add": wishAdd(); break;
      case "wish-claim":
        api("/wishlist/" + encodeURIComponent(node.dataset.id) + "/claim", { method: "POST" })
          .then(function () { sfx.pop(); return refresh(); }).catch(fail);
        break;
      case "wish-done": wishDone(node.dataset.id); break;
      case "wish-del":
        api("/wishlist/" + encodeURIComponent(node.dataset.id), { method: "DELETE" })
          .then(function () { sfx.pop(); return refresh(); }).catch(fail);
        break;
      case "note-add": noteAdd(); break;
      case "note-del":
        api("/notes/" + encodeURIComponent(node.dataset.id), { method: "DELETE" })
          .then(function () { sfx.pop(); return refresh(); }).catch(fail);
        break;

      case "req-scope":
        Array.prototype.forEach.call(document.querySelectorAll("#reqScope .chip"), function (chip) { chip.classList.remove("is-active"); });
        node.classList.add("is-active");
        sfx.click();
        break;
      case "req-toggle":
        node.dataset.per = node.dataset.per === "1" ? "0" : "1";
        node.classList.toggle("is-active", node.dataset.per === "1");
        sfx.click();
        break;
      case "req-create": requestCreate(); break;
      case "req-pay":
        api("/requests/" + encodeURIComponent(node.dataset.id) + "/pay", { method: "POST", body: {} })
          .then(function () { sfx.coin(); haptic("success"); toast("Засчитано в твой вклад"); return refresh(); }).catch(fail);
        break;
      case "req-close":
        api("/requests/" + encodeURIComponent(node.dataset.id) + "/close", { method: "POST" })
          .then(function () { sfx.pop(); return refresh(); }).catch(fail);
        break;
      case "req-poke": {
        var phrases = state.data.poke_phrases || ["скинь, не позорься"];
        api("/requests/" + encodeURIComponent(node.dataset.id) + "/poke", { method: "POST" })
          .then(function (payload) {
            sfx.hop();
            haptic("success");
            toast(payload.phrase || phrases[0]);
            return refresh();
          })
          .catch(function (error) {
            if (error.code === "poke_cooldown") { toast("Уже напоминали — дай им время"); return; }
            fail(error);
          });
        break;
      }

      case "cal-member":
        state.calendarMember = node.dataset.member || null;
        sfx.click();
        loadCalendar();
        break;
      case "cal-day": openCalendarDay(node.dataset.day); break;

      case "pet-pat": patPet(); break;
      case "pick": pick(node.dataset.member); break;
      case "roulette-open": sfx.click(); openRoulette(); break;
      case "roulette-spin": spinRoulette(); break;
      case "roulette-confirm": confirmRoulette(); break;
      case "quests-open": sfx.click(); openQuests(); break;
      case "stats-open": sfx.click(); openStats(); break;
      case "cat-dialog-close": closeCatDialog(); break;

      case "share-receipt": shareReceipt(node.dataset.id); break;
      case "share-cert": shareCertificate(node.dataset.member); break;
      case "share-month": shareMonth(); break;
      case "share-streak": shareStreak(); break;
      case "share-roulette": shareRoulette(); break;
      case "share-story": shareStory(node.dataset.id); break;
      case "export": exportCsv(); break;
      default: break;
    }
  }

  function patPet() {
    sfx.hop();
    haptic("light");
    api("/pet/pat", { method: "POST" }).then(function (payload) {
      var pet = state.data.pet;
      if (pet) { pet.pats = (pet.pats || 0) + 1; }
      toast(pet && pet.phrase ? pet.phrase : "мур");
      scene.hopCat(el.stage);
      drawPanel();
      if (payload.left !== null && payload.left !== undefined && payload.left === 0) {
        toast("Кот устал мурчать, приходи завтра");
      }
    }).catch(function (error) {
      if (error.code === "pet_tired") { toast("Кот устал мурчать, приходи завтра"); return; }
      fail(error);
    });
  }

  function noteAdd() {
    var input = document.getElementById("noteText");
    var text = input ? input.value.trim() : "";
    if (!text) { toast("Напиши записку", { type: "error" }); return; }
    api("/notes", { method: "POST", body: { text: text } }).then(function () {
      sfx.pop();
      return refresh();
    }).catch(fail);
  }

  function addKey(key) {
    var calc = state.calc;
    if (key === "back") { calc.amount = calc.amount.slice(0, -1); }
    else if (key === ",") { if (calc.amount.indexOf(",") === -1 && calc.amount.length) { calc.amount += ","; } }
    else if (calc.amount.length < 9) { calc.amount += key; }
    sfx.click();
    syncAdd();
  }

  function wishAdd() {
    var input = document.getElementById("wishTitle");
    var title = input ? input.value.trim() : "";
    if (!title) { toast("Напиши, что купить", { type: "error" }); return; }
    api("/wishlist", { method: "POST", body: { title: title } }).then(function () {
      sfx.pop();
      return refresh();
    }).catch(fail);
  }

  function wishDone(itemId) {
    var item = null;
    (state.data.wishlist_open || []).forEach(function (entry) {
      if (entry.id === itemId) { item = entry; }
    });
    if (!item) { toast("Пункт не найден", { type: "error" }); return; }
    openAdd({ title: item.title, wishlistId: itemId });
  }

  function requestCreate() {
    var titleInput = document.getElementById("reqTitle");
    var amountInput = document.getElementById("reqAmount");
    var scopeNode = document.querySelector("#reqScope .chip.is-active");
    var perNode = document.querySelector('[data-act="req-toggle"]');
    var title = titleInput ? titleInput.value.trim() : "";
    var amount = amountInput ? parseFloat(String(amountInput.value).replace(",", ".")) : NaN;
    if (!title || isNaN(amount) || amount <= 0) { toast("Нужны название и сумма", { type: "error" }); return; }
    api("/requests", {
      method: "POST",
      body: {
        title: title,
        amount: amount,
        scope: scopeNode ? scopeNode.dataset.scope : "all",
        per_person: perNode ? perNode.dataset.per === "1" : true
      }
    }).then(function () { sfx.coin(); haptic("success"); return refresh(); }).catch(fail);
  }

  function exportCsv() {
    // Ссылкой не отдаём: API требует заголовок с initData, поэтому качаем сами.
    fetch(API + "/export.csv", { headers: { "X-Telegram-Init-Data": (tg && tg.initData) || "" } })
      .then(function (response) { return response.blob(); })
      .then(function (blob) {
        var url = URL.createObjectURL(blob);
        var link = document.createElement("a");
        link.href = url;
        link.download = "obshak.csv";
        document.body.appendChild(link);
        link.click();
        window.setTimeout(function () { URL.revokeObjectURL(url); link.remove(); }, 1000);
        sfx.pop();
      })
      .catch(function () { toast("Не получилось выгрузить", { type: "error" }); });
  }

  function shareReceipt(expenseId) {
    var row = null;
    (state.data.recent || []).forEach(function (item) { if (item.id === expenseId) { row = item; } });
    if (!row) { toast("Не нашёл чек", { type: "error" }); return; }
    var canvas = share.receiptCanvas({
      code: String(row.id).slice(-4).toUpperCase(),
      date: formatWhen(row.created_at),
      memberKey: row.member_id,
      memberName: memberName(row.member_id),
      item: row.title,
      amountText: money(row.amount),
      currency: currency(),
      joke: (state.data.news && state.data.news[0] && state.data.news[0].text) || "спасибо за вклад",
      totalsLabel: "всего " + money(state.data.totals_all.total) + "\u00a0" + currency()
    });
    share.shareImage(canvas, "sosedi-chek.png", row.title + " — " + money(row.amount) + "\u00a0" + currency());
  }

  function shareCertificate(memberKey) {
    var row = leaderRow(memberKey);
    var canvas = share.certificateCanvas({
      title: "Участник",
      memberKey: memberKey,
      memberName: memberName(memberKey),
      amountText: money(row.total),
      currency: currency(),
      periodLabel: "вложено за всё время • вкладок: " + row.count
    });
    share.shareImage(canvas, "sosedi-gramota.png", memberName(memberKey) + " — " + money(row.total) + "\u00a0" + currency());
  }

  function shareMonth() {
    var data = state.data;
    var spending = data.spending || {};
    var crown = data.crown;
    var leader = (data.crowns && data.crowns[0]) || null;
    var key = crown ? crown.member_id : (leader ? leader.member_id : null);
    if (!key) { toast("Пока некого короновать", { type: "error" }); return; }
    var rows = (data.leaderboard || []).slice(0, 3).map(function (row) {
      return { name: row.name, amountText: money(row.total) };
    });
    var monthLabel = new Date().toLocaleDateString("ru-RU", { month: "long", year: "numeric" });
    var canvas = share.monthCanvas({
      memberKey: key,
      memberName: memberName(key),
      amountText: money(crown ? crown.total : leader.total),
      currency: currency(),
      monthLabel: monthLabel,
      rows: rows,
      totalsLabel: "касса соседей: " + money(data.totals_all.total) + "\u00a0" + currency()
    });
    share.shareImage(canvas, "sosedi-sponsor.png", "Спонсор месяца: " + memberName(key));
  }

  function shareStreak() {
    var data = state.data;
    var streak = data.streak || { current: 0, best: 0, days: [] };
    var quest = data.quest;
    var canvas = share.streakCanvas({
      days: streak.current,
      daysLabel: pluralDays(streak.current) + " подряд",
      best: streak.best,
      hot: streak.current > 0,
      cells: (streak.days || []).map(function (day) { return !!day.hit; }),
      questTitle: quest ? quest.title : "квест недели",
      questLine: quest ? (quest.done ? "выполнен" : "прогресс " + quest.progress + "/" + quest.target) : "",
      questDone: !!(quest && quest.done),
      totalsLabel: "касса: " + money(data.totals_all.total) + "\u00a0" + currency()
    });
    share.shareImage(canvas, "sosedi-ogon.png", "Огонь соседей: " + streak.current + " " + pluralDays(streak.current));
  }

  function shareRoulette() {
    var data = state.data;
    var roulette = data.roulette || { stats: { counts: {} } };
    var winner = (state.roulette && state.roulette.winner) || (roulette.stats.last && roulette.stats.last.member_id);
    if (!winner) { toast("Сначала крути рулетку", { type: "error" }); return; }
    var counts = roulette.stats.counts || {};
    var members = data.members || [];
    var weights = members.map(function (member) { return 1 / (1 + (counts[member.key] || 0)); });
    var total = weights.reduce(function (acc, value) { return acc + value; }, 0) || 1;
    var rows = members.map(function (member, index) {
      return { name: member.name, chanceText: Math.round(100 * weights[index] / total) + "%" };
    });
    var winnerRow = null;
    rows.forEach(function (row, index) {
      if (members[index].key === winner) { winnerRow = row; }
    });
    var canvas = share.rouletteCanvas({
      memberKey: winner,
      memberName: memberName(winner),
      chanceText: winnerRow ? "шанс был " + winnerRow.chanceText : "",
      rows: rows,
      totalsLabel: "за две недели: " + (roulette.stats.total || 0) + " " + pluralTimes(roulette.stats.total || 0)
    });
    share.shareImage(canvas, "sosedi-ruletka.png", "В магазин идёт " + memberName(winner));
  }

  function shareStory(expenseId) {
    var row = null;
    (state.data.recent || []).forEach(function (item) { if (item.id === expenseId) { row = item; } });
    if (!row) { toast("Не нашёл чек", { type: "error" }); return; }
    var data = state.data;
    var quest = data.quest;
    var crown = data.crowns && data.crowns[0];
    var canvas = share.storyCanvas({
      memberKey: row.member_id,
      memberName: memberName(row.member_id),
      item: row.title,
      amountText: money(row.amount),
      currency: currency(),
      questLine: quest ? "квест: " + quest.progress + "/" + quest.target : "",
      crownLine: crown ? "корона месяца: " + memberName(crown.member_id) : "",
      streakLine: (data.streak.current || 0) + " " + pluralDays(data.streak.current || 0) + " подряд",
      totalsLabel: "касса: " + money(data.totals_all.total) + "\u00a0" + currency()
    });
    share.shareImage(canvas, "sosedi-story.png", row.title + " — " + money(row.amount) + "\u00a0" + currency());
  }

  // --- события ----------------------------------------------------------

  function bind() {
    if (state.bound) { return; }
    state.bound = true;
    document.addEventListener("click", function (event) {
      var node = event.target.closest ? event.target.closest("[data-act]") : null;
      if (node) {
        event.preventDefault();
        sfx.unlock();
        onAction(node.dataset.act, node);
        return;
      }
      var hotspot = event.target.closest ? event.target.closest("[data-action]") : null;
      if (hotspot) {
        event.preventDefault();
        sfx.unlock();
        var action = hotspot.dataset.action;
        if (action === "member") {
          sfx.click();
          openMember(hotspot.dataset.member);
        } else if (action === "light") {
          toggleLight();
        } else {
          onAction(action + "-open", hotspot);
        }
      }
    });

    document.addEventListener("keydown", function (event) {
      if (event.key !== "Enter" && event.key !== " ") { return; }
      var node = event.target.closest ? event.target.closest("[data-action]") : null;
      if (!node) { return; }
      event.preventDefault();
      if (node.dataset.action === "member") { openMember(node.dataset.member); }
      else if (node.dataset.action === "light") { toggleLight(); }
      else { onAction(node.dataset.action + "-open", node); }
    });

    el.periodBar.addEventListener("click", function (event) {
      var node = event.target.closest(".period");
      if (!node) { return; }
      state.period = node.dataset.period;
      sfx.click();
      refresh().catch(fail);
    });

    el.totalBtn.addEventListener("click", function () { sfx.click(); openRating(); });
    el.soundBtn.addEventListener("click", function () {
      var on = sfx.toggle();
      el.soundBtn.classList.toggle("is-off", !on);
      el.soundBtn.textContent = on ? "♪" : "×";
    });
    el.helpBtn.addEventListener("click", function () { sfx.click(); openHelp(); });

    document.addEventListener("visibilitychange", function () {
      if (!document.hidden) { refresh().catch(function () { }); }
    });
  }

  // --- запуск -----------------------------------------------------------

  function init() {
    el.app = document.getElementById("app");
    el.stage = document.getElementById("stage");
    el.shelf = document.getElementById("shelf");
    el.periodBar = document.getElementById("periodBar");
    el.homeBars = document.getElementById("homeBars");
    el.totalBtn = document.getElementById("totalBtn");
    el.totalValue = document.getElementById("totalValue");
    el.totalCur = document.getElementById("totalCur");
    el.soundBtn = document.getElementById("soundBtn");
    el.helpBtn = document.getElementById("helpBtn");
    el.panelRoot = document.getElementById("panelRoot");
    el.pickerRoot = document.getElementById("pickerRoot");
    el.toastRoot = document.getElementById("toastRoot");
    el.statusStrip = document.getElementById("statusStrip");
    el.budgetBar = document.getElementById("budgetBar");
    el.budgetLabel = document.getElementById("budgetLabel");
    el.budgetFill = document.getElementById("budgetFill");
    el.budgetValue = document.getElementById("budgetValue");
    el.catRoot = document.getElementById("catRoot");
    el.fxCanvas = document.getElementById("fxCanvas");
    el.boot = document.getElementById("boot");
    el.bootText = document.getElementById("bootText");
    el.brandMark = document.getElementById("brandMark");

    el.brandMark.innerHTML = art.brand();
    el.app.setAttribute("data-time", timeOfDay());
    try {
      state.light = window.localStorage.getItem("obshak.light") === "off" ? "off" : "on";
    } catch (error) { state.light = "on"; }
    applyLight();
    bind();

    if (tg) {
      try {
        tg.ready();
        tg.expand();
        if (tg.setHeaderColor) { tg.setHeaderColor("#0e0f16"); }
        if (tg.setBackgroundColor) { tg.setBackgroundColor("#0e0f16"); }
        if (tg.disableVerticalSwipes) { tg.disableVerticalSwipes(); }
        state.startParam = (tg.initDataUnsafe && tg.initDataUnsafe.start_param) || "";
      } catch (error) { /* ignore */ }
    }

    window.setInterval(function () { el.app.setAttribute("data-time", timeOfDay()); }, 60000);

    if (!tg || !tg.initData) {
      showBootError("Открой «Соседей» кнопкой в беседе с Тимуром");
      renderPicker();
      setPanel({ title: "НЕТ ДОСТУПА", body: function () {
        return '<div class="empty">Миниапп открывается из Telegram. Зайди в беседу и нажми «Открыть Соседей».</div>';
      } });
      return;
    }

    loadInitial();
  }

  function loadInitial() {
    api("/bootstrap?period=all").then(function (payload) {
      state.data = payload;
      state.me = payload.me;
      state.period = payload.period || "all";
      state.booted = true;
      hideBoot();
      renderChrome();
      renderScene();
      loadWeather();
      syncLight();
      window.setTimeout(showCatDialog, 1100);
      if (state.startParam === "add") { openAdd(); }
    }).catch(function (error) {
      if (error.code === "needs_link") {
        state.booted = true;
        hideBoot();
        renderPicker(error.payload && error.payload.members);
        return;
      }
      fail(error);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
