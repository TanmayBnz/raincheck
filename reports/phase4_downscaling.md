# Phase-4 Downscaling QA — L2b

_Generated 2026-08-27 09:30 UTC. 2,875,844 intervals, each carrying both the native ~31 km hourly ERA5 label and the spateGAN 2 km / 10 min label._

**Gate verdict: PASS**

## 1. Did downscaling actually add spatial information?

The prerequisite for everything else. At native resolution Manchester and
Essen each collapsed to a **single ERA5 cell**, so every detector in the
city was assigned identical rainfall — there was no within-city variation
to exploit at all. If the 2 km fields do not disagree across detectors,
the downscaling is decoration.

| city | wet timestamps | detectors disagree on band | mean within-city spread (mm/h) | max spread |
|---|---|---|---|---|
| manchester | 527 | 70.2% | 1.25 | 24.0 |
| torino | 508 | 93.1% | 1.14 | 16.6 |
| essen | 636 | 67.5% | 1.15 | 27.4 |

## 2. Dose-response: coarse vs downscaled, same intervals

Mean `typical_speed_deviation` by band, in **percentage points relative to
Dry**. Negative means slower than this detector normally is in this hour of
this weekday. Both labellings sit on the same rows, so this is a paired
comparison in which the only thing that changed is resolution.

**Native ~31 km / hourly:**

| city | Light | Moderate | Heavy | Extreme | **Moderate+** |
|---|---|---|---|---|---|
| manchester | +4.6 | -1.9 | — | — | **-1.9** |
| torino | +1.5 | -2.0 | — | — | **-2.0** |
| essen | +0.8 | +1.4 | — | — | **+1.4** |

**Downscaled 2 km / 10 min:**

| city | Light | Moderate | Heavy | Extreme | **Moderate+** |
|---|---|---|---|---|---|
| manchester | -0.0 | +0.1 | +2.3 | +4.1 | **+0.6** |
| torino | -0.4 | -1.8 | -2.8 | -8.0 | **-2.0** |
| essen | +1.3 | -0.2 | -0.3 | — | **-0.2** |

**Moderate+ contrast, side by side** (more negative = rain effect resolved
more sharply):

| city | 31 km / 1 h | 2 km / 10 min | change | sharper? |
|---|---|---|---|---|
| manchester | -1.9 | +0.6 | +2.6 | no |
| torino | -2.0 | -2.0 | +0.1 | no |
| essen | +1.4 | -0.2 | -1.6 | yes |

## 3. Two-channel decomposition — and the demand confound

The pooled numbers above are hard to read because rain does not do one
thing. It slows vehicles, and it also **removes** them: fewer trips are
taken, and on a signalised arterial fewer vehicles means higher speeds.
Those two effects work in opposite directions on the pooled average, which
is why a positive Moderate+ figure is not evidence that rain speeds traffic
up.

Splitting by whether the detector was above its own critical occupancy
separates them, and carrying Δflow alongside Δspeed makes the demand
channel visible directly: if flow falls while speed rises, the road got
emptier, not faster.

| city | road state | dry n | Moderate+ n | Moderate+ Δspeed (pp) | Moderate+ Δflow (pp) |
|---|---|---|---|---|---|
| manchester | free-flowing | 358,271 | 21,045 | +0.9 | -6.4 |
| manchester | congested | 78,314 | 4,285 | -1.7 | -0.3 |
| torino | free-flowing | 1,240,377 | 44,006 | -0.7 | +0.5 |
| torino | congested | 156,142 | 9,109 | -2.7 | -1.5 |
| essen | free-flowing | 113,290 | 5,014 | -0.0 | -7.0 |
| essen | congested | 1,090 | 62 | -9.9 | -2.7 |

## 4. Rain onset

Speed deviation by minutes since rain began, in percentage points relative
to dry intervals (counts in brackets). Driver adaptation theory predicts the
first minutes are disproportionately disruptive.

This table cannot be produced at native resolution at all: an hourly label
cannot separate the first ten minutes of rain from its fifth hour. It is the
clearest single justification for the 10-minute fields.

| city | 0-10 min | 10-30 min | 30-60 min | 60+ min |
|---|---|---|---|---|
| manchester | +2.2 (3,865) | +3.8 (3,231) | +2.2 (3,754) | -0.4 (29,768) |
| torino | +0.3 (8,888) | +0.6 (9,278) | -0.7 (7,190) | -1.7 (73,969) |
| essen | -1.0 (1,017) | +2.4 (1,224) | +0.9 (1,320) | +0.1 (5,266) |

## 5. Dry-spell antecedent

Deviation at rain onset, bucketed by how long it had been dry beforehand.
The documented oil-film effect predicts a larger slowdown after a long dry
spell.

| city | < 6 h | 6-24 h | 1-3 d | 3+ d |
|---|---|---|---|---|
| manchester | +6.6 (955) | +5.0 (660) | +6.2 (332) | — |
| torino | +0.6 (906) | +0.9 (1,773) | +1.5 (1,377) | +3.7 (215) |
| essen | +0.3 (299) | -1.1 (178) | -0.3 (113) | — |

## 6. Caveats

- **One ensemble member.** spateGAN is probabilistic and CONTEXT.md §L2b
  specifies ensemble spread as an uncertainty covariate. Only seed 10 was
  run; `run_downscaling --seed N` produces further members, and the feature
  pipeline is member-agnostic. Until then there is no spread column.
- **The downscaler is not observation.** These are plausible high-resolution
  realisations conditioned on ERA5, not measurements. No radar ground truth
  exists for these cities, so the honest framing (CONTEXT.md §9) is a
  realisation, and the ablation in this report is the evidence it helps.
- **Germany is in-domain, the UK and Italy are not.** spateGAN was trained on
  German radar, so Essen is the in-domain anchor and Manchester and Torino
  are out-of-domain generalisation tests.
