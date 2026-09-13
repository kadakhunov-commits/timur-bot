/* Чек и грамота рисуются на canvas: их можно отправить в беседу картинкой. */
(function () {
  "use strict";

  function ready() {
    try {
      if (document.fonts && document.fonts.load) {
        return Promise.all([
          document.fonts.load('28px "Press Start 2P"'),
          document.fonts.load('24px "Pixelify Sans"')
        ]).catch(function () { });
      }
    } catch (error) { /* шрифты не критичны */ }
    return Promise.resolve();
  }

  function surface(width, height, background) {
    var canvas = document.createElement("canvas");
    canvas.width = width;
    canvas.height = height;
    var ctx = canvas.getContext("2d");
    ctx.imageSmoothingEnabled = false;
    ctx.fillStyle = background;
    ctx.fillRect(0, 0, width, height);
    return { canvas: canvas, ctx: ctx };
  }

  function pixelFont(ctx, size) {
    ctx.font = size + 'px "Press Start 2P", ui-monospace, monospace';
  }

  function bodyFont(ctx, size, weight) {
    ctx.font = (weight || 600) + " " + size + 'px "Pixelify Sans", system-ui, sans-serif';
  }

  function dashed(ctx, x1, y, x2, color) {
    ctx.strokeStyle = color;
    ctx.lineWidth = 3;
    ctx.setLineDash([10, 8]);
    ctx.beginPath();
    ctx.moveTo(x1, y);
    ctx.lineTo(x2, y);
    ctx.stroke();
    ctx.setLineDash([]);
  }

  function drawAvatar(ctx, key, x, y, scale) {
    var art = window.ObshakArt;
    var rects = art.avatarData(key);
    for (var i = 0; i < rects.length; i += 1) {
      var rect = rects[i];
      ctx.fillStyle = art.PALETTE[rect[4]] || "#000";
      ctx.fillRect(x + rect[0] * scale, y + rect[1] * scale, rect[2] * scale, rect[3] * scale);
    }
  }

  function drawBadge(ctx, icon, x, y, scale, color) {
    var art = window.ObshakArt;
    var rects = art.mapToRects(art.map[icon] || art.map.star);
    for (var i = 0; i < rects.length; i += 1) {
      var rect = rects[i];
      if (rect[4] === "w") { ctx.fillStyle = "#2a2205"; }
      else if (rect[4] === "d") { ctx.fillStyle = art.shade(color, -70); }
      else { ctx.fillStyle = color; }
      ctx.fillRect(x + rect[0] * scale, y + rect[1] * scale, rect[2] * scale, rect[3] * scale);
    }
  }

  function receiptCanvas(data) {
    var width = 520;
    var height = 660;
    var made = surface(width, height, "#f4efe0");
    var ctx = made.ctx;

    ctx.strokeStyle = "#c9bb9a";
    ctx.lineWidth = 6;
    ctx.strokeRect(14, 14, width - 28, height - 28);

    ctx.fillStyle = "#2b2f3d";
    pixelFont(ctx, 30);
    ctx.textAlign = "center";
    ctx.fillText("СОСЕДИ", width / 2, 86);

    bodyFont(ctx, 22, 600);
    ctx.fillStyle = "#6d6350";
    ctx.fillText("чек " + (data.code || "—") + " • " + (data.date || ""), width / 2, 122);

    dashed(ctx, 60, 150, width - 60, "#c9bb9a");

    drawAvatar(ctx, data.memberKey, 60, 186, 5);
    bodyFont(ctx, 30, 700);
    ctx.fillStyle = "#201b12";
    ctx.textAlign = "left";
    ctx.fillText(data.memberName || "", 168, 226);

    bodyFont(ctx, 40, 700);
    ctx.textAlign = "center";
    ctx.fillText(String(data.item || "").slice(0, 22), width / 2, 340);

    pixelFont(ctx, 34);
    ctx.fillStyle = "#7a2f1f";
    ctx.fillText(data.amountText + " " + data.currency, width / 2, 420);

    dashed(ctx, 60, 470, width - 60, "#c9bb9a");

    bodyFont(ctx, 24, 600);
    ctx.fillStyle = "#6d6350";
    var joke = data.joke || "";
    ctx.fillText(joke.slice(0, 34), width / 2, 522);

    ctx.textAlign = "left";
    bodyFont(ctx, 20, 600);
    ctx.fillStyle = "#8a8070";
    ctx.fillText("спасибо за вклад", 60, 596);
    ctx.textAlign = "right";
    ctx.fillText(data.totalsLabel || "", width - 60, 596);
    ctx.textAlign = "center";

    return made.canvas;
  }

  function certificateCanvas(data) {
    var width = 640;
    var height = 480;
    var made = surface(width, height, "#1a1c26");
    var ctx = made.ctx;

    ctx.strokeStyle = "#ffd447";
    ctx.lineWidth = 6;
    ctx.strokeRect(18, 18, width - 36, height - 36);
    ctx.lineWidth = 2;
    ctx.strokeRect(30, 30, width - 60, height - 60);

    ctx.textAlign = "center";
    pixelFont(ctx, 16);
    ctx.fillStyle = "#8b90a3";
    ctx.fillText("ГРАМОТА СОСЕДЕЙ", width / 2, 78);

    pixelFont(ctx, 26);
    ctx.fillStyle = "#ffd447";
    ctx.fillText((data.title || "СПОНСОР МЕСЯЦА").toUpperCase(), width / 2, 128);

    drawAvatar(ctx, data.memberKey, width / 2 - 64, 162, 8);

    bodyFont(ctx, 46, 700);
    ctx.fillStyle = "#eef1f7";
    ctx.fillText(data.memberName || "", width / 2, 366);

    bodyFont(ctx, 30, 700);
    ctx.fillStyle = "#ffd447";
    ctx.fillText(data.amountText + " " + data.currency, width / 2, 404);

    bodyFont(ctx, 20, 600);
    ctx.fillStyle = "#8b90a3";
    ctx.fillText(data.periodLabel || "", width / 2, 432);

    return made.canvas;
  }

  function download(canvas, filename) {
    try {
      var url = canvas.toDataURL("image/png");
      var link = document.createElement("a");
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      window.setTimeout(function () { link.remove(); }, 800);
      return true;
    } catch (error) {
      return false;
    }
  }

  function shareImage(canvas, filename, text) {
    return ready().then(function () {
      return new Promise(function (resolve) {
        canvas.toBlob(function (blob) {
          if (!blob) { resolve(false); return; }
          var file = null;
          try { file = new File([blob], filename, { type: "image/png" }); } catch (error) { file = null; }
          if (file && navigator.canShare && navigator.canShare({ files: [file] }) && navigator.share) {
            navigator.share({ files: [file], text: text || "" }).then(
              function () { resolve(true); },
              function () { download(canvas, filename); resolve(false); }
            );
            return;
          }
          download(canvas, filename);
          resolve(false);
        }, "image/png");
      });
    });
  }

  window.ObshakShare = {
    ready: ready,
    receiptCanvas: receiptCanvas,
    certificateCanvas: certificateCanvas,
    shareImage: shareImage,
    download: download
  };
})();
