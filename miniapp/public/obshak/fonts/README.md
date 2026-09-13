# Шрифты общака

Лежат локально, чтобы кириллица не зависела от чужого CDN: на Google Fonts она
отдаётся отдельным файлом, который грузится лениво и при сбое подменялся
системным шрифтом — текст от этого выглядел «кривым».

| Файл | Шрифт | Набор |
| --- | --- | --- |
| `press-start-2p-latin.woff2` | Press Start 2P | latin |
| `press-start-2p-cyrillic.woff2` | Press Start 2P | cyrillic |
| `pixelify-sans-latin.woff2` | Pixelify Sans (variable, 400–700) | latin |
| `pixelify-sans-cyrillic.woff2` | Pixelify Sans (variable, 400–700) | cyrillic |

Оба шрифта распространяются по лицензии SIL Open Font License 1.1, которая
разрешает использование и встраивание в том числе в коммерческих проектах.
Исходники: https://fonts.google.com/specimen/Press+Start+2P и
https://fonts.google.com/specimen/Pixelify+Sans

`@font-face` для них — в начале `../app.css`. `unicode-range` скопированы из
раздачи Google Fonts, поэтому latin и cyrillic грузятся как отдельные наборы.
