# Serval OCR Accuracy Notes

This document records the OCR accuracy experiments run while developing `serval ocr`.
The goal of these experiments was not just to maximize total recovered datetimes, but to
separate high-confidence OCR results from rows that should be reviewed by an LLM or a
human.

## Dataset

Evaluation was done against a test set of 1433 cropped watermark images.

Local artifacts used during development:

- OCR baseline: `serval_output/serval_ocr_baseline/ocr.csv`
- OCR with allowed chars: `serval_output/serval_ocr_allowed_chars/ocr.csv`
- OCR with allowed chars and crop: `serval_output/serval_ocr_allowed_chars_with_crop/ocr.csv`
- Crop images for LLM labeling: `serval_output/serval_ocr_allowed_chars_with_crop/debug_crops`
- LLM-labeled ground truth: `serval_output/llm_ground_truth_gpt4o_mini.csv`

The LLM truth file contains one row per crop and uses normalized timestamps in
`yyyy-mm-dd hh:mm:ss` form. The labeling prompt asked the vision model to output only that
format. API keys used during the experiment are intentionally not recorded here.

## Baseline Strategies

Initial comparisons used the same 1433-row truth set.

| Strategy | Correct | Missing | Wrong | Accuracy |
| --- | ---: | ---: | ---: | ---: |
| Baseline OCR | 1158 | 226 | 49 | 80.809% |
| Allowed chars | 1195 | 184 | 54 | 83.391% |
| Allowed chars + crop | 1234 | 180 | 19 | 86.113% |

Conclusions:

- Crop is the largest straightforward improvement. It reduces irrelevant OCR text and
  improves both speed and accuracy.
- Allowed chars helped on this dataset and should be the default for datetime OCR.
- The old `O/I/l` cleanup was not useful for the allowed-chars path because allowed-chars
  output did not contain those letters. Keeping that cleanup in strict parsing risks
  over-correction.

## Repair Experiments

Repair was tested as an explicit opt-in mode, not as part of the confident result set.

The implemented repair strategy:

- Run only when strict parsing fails.
- Find a year-like position.
- Collect the first 14 digits from that point.
- Reformat as `yyyy-mm-dd hh:mm:ss`.
- Validate with `chrono`.
- If `--year-range` is supplied, only accept years in range or a unique single-digit
  correction into the range.

Evaluation from existing OCR text:

| Strategy | Correct | Missing | Wrong | Accuracy |
| --- | ---: | ---: | ---: | ---: |
| Strict from crop OCR text, no repair | 1231 | 184 | 18 | 85.904% |
| Strict + repair | 1383 | 24 | 26 | 96.511% |
| Strict + repair + `--year-range 2025..2026` | 1390 | 23 | 20 | 96.999% |

Repair substantially improves recovery, but it also creates false positives. On this data,
plain repair produced 160 repaired rows: 152 correct and 8 wrong. Therefore repair output
is useful as a candidate value or LLM hint, but should not be classified as assured.

The year-range rule is deliberately conservative:

- It can reject impossible years such as `2125` when the expected range is `2025..2026`.
- It can correct a unique one-digit year error such as `2125 -> 2025`.
- It does not guess when both years in range are plausible.

## Confident Result Evaluation

For user-facing output, the key metric is precision of the rows marked confident.

`confident` was initially defined as:

- strict datetime parse succeeds;
- year is in the user-provided range when `--year-range` is set;
- no repair was used.

On the crop + allowed-chars dataset:

| Confident rule | Confident rows | Correct | Wrong | Precision | Recall |
| --- | ---: | ---: | ---: | ---: | ---: |
| Strict, any year | 1249 | 1231 | 18 | 98.559% | 85.904% |
| Strict + `2025..2026` year range | 1243 | 1231 | 12 | 99.035% | 85.904% |
| Strict + year range + digit boundaries | 1194 | 1183 | 11 | 99.079% | 82.554% |
| Strict + year range + OCR text exactly equals datetime | 1183 | 1172 | 11 | 99.070% | 81.786% |

Boundary and exact-text checks removed many correct rows while barely improving precision.
They are not good default confident criteria.

The remaining false positives after strict + year-range were legal-looking datetimes with
one wrong digit. Examples:

| OCR result | Truth | Error type |
| --- | --- | --- |
| `2026-01-13 16:23:16` | `2026-01-03 16:23:16` | day digit |
| `2025-12-07 11:24:12` | `2025-12-07 10:24:12` | hour digit |
| `2025-12-28 15:14:21` | `2025-12-28 15:14:20` | second digit |

These cannot be identified reliably from a single strict parse alone.

## Multiple OCR Cross-Check

A stronger signal came from comparing different OCR configurations for the same media:

- crop + allowed chars;
- full image + allowed chars;
- full image baseline.

This is computationally more expensive, but useful as a research reference.

| Rule | Rows | Correct | Wrong | Precision |
| --- | ---: | ---: | ---: | ---: |
| crop strict + year range | 1243 | 1231 | 12 | 99.035% |
| crop result agrees with full-image allowed-chars result | 1064 | 1064 | 0 | 100.000% |
| crop result agrees with full-image baseline result | 1031 | 1031 | 0 | 100.000% |
| crop result agrees with either full-image result | 1064 | 1064 | 0 | 100.000% |
| all three OCR results agree | 1031 | 1031 | 0 | 100.000% |

The important product conclusion is not to use majority vote for automatic correction.
When OCR configurations disagree, the row should be downgraded for LLM review. In this
dataset, conflict rows contained both crop-correct and full-image-correct cases.

Because this cross-check roughly doubles OCR work, it is not the preferred default path.

## Sequence Outlier Check

A lower-cost alternative is to use the batch's local sequence context after OCR.

The implemented `--sequence-outlier-check` is a conservative downgrade-only rule:

- Group rows by parent directory.
- Sort each group by filename.
- For a strict confident row, find the nearest previous and next rows with parsed datetimes,
  looking up to four positions away on each side.
- If those two neighbors are close to each other (`<= 15 minutes`) and the current row is
  far from both (`> 30 minutes`), downgrade it to `needs_llm`.
- Do not change `datetime_ocr`.

This catches examples like:

```text
IMG_0796 -> 2026-01-03 16:23:16
IMG_0797 -> 2026-01-13 16:23:16  # downgraded
IMG_0798 -> 2026-01-03 16:27:06
```

On the current data, the conservative sequence rule is expected to remove about 3 of the
12 strict+year-range false positives with very little cost. More aggressive sequence
clustering can remove all 12 observed false positives, but it also downgrades hundreds of
correct rows. That is too aggressive for the default behavior.

## Current Recommended Classification

Recommended output classes:

- `confident`: strict parse succeeds, year range passes, and no repair was used.
- `needs_llm`: parse failed, OCR failed, repair-only output, year out of range, or
  sequence outlier.

Optional future refinements:

- Add a separate `candidate` class for repair results when the caller wants to pass hints
  to an LLM.
- Add an opt-in cross-check mode when users prefer higher precision over OCR cost.
- Record sequence outlier parameters in CLI options if more datasets show that different
  camera folders need different thresholds.

The current code reflects this direction by writing `confidence` and `confidence_reason`
columns and by making sequence outlier checking opt-in.
