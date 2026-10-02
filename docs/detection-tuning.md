# Detection tuning log

The detection-quality gate (`dbt/sentinel/tests/assert_detection_quality.sql`) fails the
build when any rule's precision or recall drops below 0.8 against the simulator's ground
truth. This page records what it caught and what changed as a result.

## 1. Brute force: threshold raised from 10 to 15 failures per 5 minutes

**Symptom.** The first end-to-end run failed the gate:

| rule | attacks | detected | incidents | precision | recall |
|---|---:|---:|---:|---:|---:|
| brute_force | 12 | 12 | 28 | 0.43 | 1.00 |

Recall was perfect, but 16 of 28 incidents were not attacks.

**Diagnosis.** Every false positive was a single account failing from its usual IP and
device, 10 to 14 times, with attempts spaced 5 to 25 seconds apart and nothing else
unusual. That is the simulator's "forgot my password after a reset" behaviour (hard negative
#2 in `simulator.py`): a real person retrying by hand, not a script.

Real brute-force campaigns in the data looked very different: 30 to 80 attempts spaced 1 to
6 seconds apart, so each attack put 30 or more failures into its busiest window.

**Fix.** Raise `brute_force_min_failures` from 10 to 15. That sits above the most a person
retries by hand and well below the least an automated attack produced, so it separates the
two groups with margin on both sides.

| rule | attacks | detected | incidents | precision | recall |
|---|---:|---:|---:|---:|---:|
| brute_force | 12 | 12 | 12 | 1.00 | 1.00 |

**Guarding against regression.** `test_brute_force_ignores_a_forgetful_user` pins this
behaviour in the unit tests, and the dbt gate re-checks it on every CI run.

**What would come next with real data.** A fixed count is easy to evade by slowing down. The
stronger signals are "new device or new IP for this account" and the time between attempts.
Both need per-account history, which belongs in the batch layer or in an arbitrary stateful
streaming operator (`applyInPandasWithState`).

## 2. Impossible travel: crediting takeovers from other campaigns

**Symptom.** Impossible travel showed 0.30 precision: 60 incidents, only 18 matched an
`impossible_travel` attack label.

**Diagnosis.** The other 42 were not false alarms. When a brute-force, stuffing or spray
attack *succeeded*, the attacker's login came from a data-centre city abroad. The victim's
next normal login from Texas then formed an impossible pair. The detector had caught real
account takeovers; the scoring only credited matches of its own attack type.

**Fix.** Score an impossible-travel incident as a true positive when *either* leg of the pair
is attacker activity, and record which campaign it matched (`matched_attack_type`). Recall
still counts only `impossible_travel` attacks, so cross-campaign catches cannot inflate it.

This is a property worth keeping: impossible travel works as a second, independent signal
that catches successful takeovers the volumetric rules flagged but could not confirm.
