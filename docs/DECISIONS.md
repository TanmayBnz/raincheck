# Decision Log

Plain-English record of technical decisions that would otherwise be confusing later.
Newest first.

---

## 2026-10-17 — Rebuilt the project on a new machine; the "busy roads" headline withdrawn; scope cut for delivery

**What changed:**
The whole data pipeline was rebuilt from the original raw files on a different
computer, and the long-planned correction to the road-state analysis was finally
run. The correction removed the project's headline finding.

**Decisions and why:**
- **Rebuild from raw files rather than hunt for the old processed data.** The
  processed data existed only on the old machine. Rebuilding took one afternoon
  and doubled as a reproducibility test: every stage reproduced the earlier
  numbers exactly (2,875,844 cleaned readings; identical baseline, rainfall and
  rain-effect tables).
- **Fix the road-state analysis before building anything on top of it.** Roads
  had been sorted into "busy" and "free-flowing" using traffic measured during
  the rain itself, but rain changes how busy a road is. Sorting instead by how
  busy each road normally is at that hour removed the apparent effect: busy
  roads no longer show any measurable speed change in rain
  (`reports/phase5R_expected_state.md`). The earlier claim that busy roads speed
  up in rain is withdrawn. What survives: free-flowing roads slow slightly in
  light and heavy rain.
- **Cut scope to what the reviewer will actually see.** The reviewer has seen
  the deck, not the codebase. Expanding to 39 cities, a message-queue streaming
  layer and a dedicated serving database were all dropped; each would cost days
  of manual work and none would change what is presented. Remaining work: the
  with/without-rain model comparison, a simple road-network analysis, and one
  dashboard page. A live-feed replay is a bonus if time allows.

**Lessons:**
- **The computer's clock setting silently broke the rainfall join.** This
  machine runs on Indian time; the original ran on UTC. Every rainfall
  timestamp shifted by five and a half hours and no rain matched any traffic
  reading. The pipeline now forces UTC itself rather than relying on the
  machine. Lesson: anything that depends on the environment should be pinned in
  code, not assumed.
- **Re-running report generators overwrote hand-written notes.** One report
  carried a hand-added correction notice that the generator does not know about;
  a rerun erased it (restored from version control). Lesson: generated reports
  and hand-written commentary should not live in the same file.
- **A known flaw left "to fix later" was carrying the headline.** The flaw was
  logged in August and the headline was presented anyway. Lesson: a headline
  that depends on an open defect is not a headline yet.

**Open decision:**
Whether to keep "GraphX" as the label for the road-network work. GraphX is not
usable from Python; the plan is a plain Python graph library, labelled honestly.

---

## 2026-08-28 — Mentor-review deck built as an HTML page, not by editing the PPTX template

**Decision:**
The presentation lives at `deck/build/raincheck-deck.html` — a self-contained HTML
deck of 29 fixed 16:9 slides — rather than as an edited copy of the Slidesgo
`.pptx` in `deck/template/`. The template was used as a *visual* source: its theme
colours (navy `#0E2A47`, steel `#869FB2`, the blue/cyan accents) and its embedded
typeface (Albert Sans) were extracted from the PPTX XML and carried across.

**Why:**
The brief asked for the template's design language, not the template's file format.
Neither `python-pptx` nor LibreOffice is installed, so a generated PPTX could not
have been rendered and checked — it would have been written blind. The HTML deck
can be opened, measured and screenshotted, which is how the slide-overflow pass
below was actually done. Albert Sans and IBM Plex Mono are both on Google Fonts, so
the HTML version reproduces the template's typography exactly rather than
approximating it.

**Alternatives considered:**
- **Edit the PPTX directly with python-pptx** — would need installing the library,
  and cloning a Slidesgo master's layouts programmatically is fiddly and produces
  worse-looking output than the template it started from. No way to verify the
  result without a renderer.
- **Export slides as images and reassemble** — same rendering problem, and the
  result is not editable by anyone.

**Trade-offs:**
Better: full control of layout, verified rendering, light/dark theming, keyboard
navigation, a shareable link, and a print stylesheet that emits 10in × 5.625in
pages for PDF. Worse: it is not a PowerPoint file. If the deck must be submitted as
`.pptx` or hand-edited in PowerPoint, print to PDF and import, or the deck has to be
rebuilt in the original format.

**Status:**
Accepted

---

## 2026-08-28 — Chart palette validated by script rather than chosen by eye

**Decision:**
The two colour encodings in the deck were run through a colour-blindness and
contrast validator before use, and re-stepped until they passed:

- completed vs upcoming — `#0891B2` teal / `#C2610C` amber (light),
  `#1FA3BE` / `#D07C28` (dark)
- speed vs flow series — `#0B7FC7` blue / `#C05A0A` orange (light),
  `#2E8FD4` / `#D06A18` (dark)

The first teal candidates (`#0E7490`, `#14607F`) failed a chroma floor — they read
as grey rather than as a colour — and were replaced.

**Why:**
The Objectives slide has to communicate "done" versus "upcoming" without the
audience re-reading a legend, so the two states must be separable for a colour-blind
viewer and on a washed-out projector. Colour and shape are both used: filled disc
with a tick for done, hollow ring for upcoming, so the split survives a monochrome
print too.

**Alternatives considered:**
- **Green / amber** — the conventional done/pending pair, but green-amber is exactly
  the pair deuteranopic viewers lose. Rejected.
- **Two shades of the template blue** — safe but too low-contrast to read at a glance.

**Trade-offs:**
The amber is warmer than the template's own cool palette, which is a deliberate
departure; it is the cost of having the two states separable at a glance.

**Status:**
Accepted
