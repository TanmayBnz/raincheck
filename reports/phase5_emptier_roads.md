# Does "fewer cars" explain why light rain looks faster?

_Generated 2026-08-27 15:02 UTC._

**Answer: Half of it. Rain really does empty roads and that really does raise speed -- but the effect does NOT track the light-rain result, so it is not the explanation for why drizzle looks faster.**

## 1. The claim being tested

Rain does two things to a road at once, and they move speed in opposite
directions. It makes drivers slower. It also makes some of them not
drive, and a road with fewer cars on it is a faster road. If the second
effect is bigger than the first, measured speed goes **up** under rain
without rain having made anyone quicker.

Both channels are already recorded per interval -- speed against this
detector's normal for this hour, and car count against the same normal --
so the claim is checkable rather than merely plausible.

## 2. Test 1 -- when speed rises, does traffic fall?

All 121 published Phase 5 contrasts where both channels were
estimable, sorted into the four sign combinations:

| | traffic down | traffic up |
|---|---|---|
| **speed up** | 36 | 11 |
| **speed down** | 40 | 34 |

The top-left cell is what the explanation predicts. The top-right is the
one that would refute it outright: more cars *and* more speed cannot be
explained by an emptier road.

Of the 47 contrasts where speed rose, **77%** also show
traffic falling.

## 3. Test 2 -- does a bigger drop in traffic mean a bigger rise in speed?

Rank correlation between the two channels across contrasts. Negative
means the two move opposite ways, which is what the explanation needs.
Ranks rather than raw values, so a couple of extreme cells cannot carry
the result.

| set of contrasts | n | correlation |
|---|---|---|
| all bands | 121 | -0.43 |
| Light only | 32 | -0.02 |

## 4. Test 3 -- split rainy intervals by whether traffic was actually lighter

If emptier roads are the mechanism, the speed rise should sit almost
entirely in the half where traffic was lighter than normal, and should
largely disappear in the half where it was not.

Speed effect against dry, percentage points:

| band | traffic lighter than normal | traffic normal or heavier |
|---|---|---|
| Light | -0.6 ns | -0.6 |
| Moderate | -1.0 | -1.4 |
| Heavy | -0.4 ns | -1.9 ns |
| Extreme | +1.8 ns | -2.8 |

`ns` marks an interval that includes zero.

**Caveat, and it is a real one.** Traffic volume is itself changed by
rain, so splitting on it means splitting on something the rain caused.
A split like this can produce a difference on its own even when the
mechanism is absent. Test 3 corroborates tests 1 and 2; it cannot carry
the conclusion alone, and nothing below leans on it.

## 5. What this does and does not establish

Two claims, scored separately, because the first run conflated them and
reached a misleading answer: the general mechanism is well supported,
and that was allowed to stand in for the light-rain case, which is the
one the question was actually about.

**Does rain empty roads, and does that raise speed?** 2/2 tests say yes.

**Is that why light rain looks faster?** 0/2 tests say yes.

- GENERAL: where speed rises, traffic falls in 77% of contrasts. The combination that would refute the explanation -- more cars and more speed -- appears 11 times.
- GENERAL: across all bands the two channels oppose each other (rank correlation -0.43) -- the bigger the drop in cars, the bigger the rise in speed.
- DRIZZLE: within light rain alone the rank correlation is -0.02 across 32 contrasts -- no relationship. The drizzle results with the biggest fall in traffic are not the ones with the biggest rise in speed.
- DRIZZLE: splitting light rain by whether traffic was actually lighter changes the speed effect by +0.04 pp, which is nothing. If emptier roads were driving the light-rain result, this split should have separated it.

It does **not** establish that rain has no slowing effect. The opposite,
if anything: the slowing effect is being masked by an opposing effect on
how many cars are on the road, and the two have to be separated before
either can be read. That is what `typical_flow_deviation` is for, and it
is why Phase 5 reports both channels side by side.

It also does not leave the light-rain result explained. Both
light-rain-specific checks came back null, so whatever makes drizzle
look faster is still unidentified -- and note that this analysis runs
on the production baseline, where the pooled light-rain effect is
mildly negative. The strongly positive light-rain readings live in
the sharp-rainfall arm and in the already-congested stratum, neither
of which this test isolates. That is the next place to look.

**Gate verdict: PASS**

The gate is on the tests having run on enough data to mean anything,
not on which way they came out.
