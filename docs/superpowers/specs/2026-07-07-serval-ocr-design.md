# Serval OCR Design

## Goal

Add a new `serval ocr <path>` command that recursively scans media files, runs OCR against each image or video first frame, extracts visible datetimes from camera-trap watermarks, and writes an independent OCR result table.

The old `ocr` branch is background only. The new implementation should be built on the current `master` structure and should not merge that branch.

## User-Facing Behavior

Command shape:

```sh
serval ocr <path> \
  --box 0.70,0.86,0.28,0.10 \
  --datetime-format "YYYY-MM-DD HH:mm:ss" \
  --allowed-chars "0123456789-/:. "
```

Arguments and options:

- `<path>`: file or directory. Directories are traversed recursively using the same media extension rules as `observe`.
- `--output <OUTPUT_DIR>`: output directory, defaulting to `./serval_output/serval_ocr`.
- `--box <x,y,w,h>`: optional crop rectangle in relative image coordinates. Each value must be between `0` and `1`. The crop is applied after image/frame extraction and before OCR.
- `--debug-crops`: save cropped input images to `<output>/debug_crops/` for inspection and box tuning.
- `--sample <N>`: process only the first `N` enumerated media files. This is primarily for quickly tuning `--box`.
- `--datetime-format <FORMAT>`: required datetime parsing format. Serval parses datetime candidates only with this format.
- `--allowed-chars <CHARS>`: optional character whitelist passed to `ocrs` recognition.

There will be no `--image` or `--video` filtering in the initial version. The command processes all supported media.

## Output

The command writes `<output>/ocr.csv` with UTF-8 BOM, matching the rest of Serval's CSV style.

Columns:

- `path`: absolute media path.
- `filename`: media filename.
- `media_type`: inferred media type.
- `datetime_ocr`: normalized datetime in `yyyy-MM-dd HH:mm:ss` when parsing succeeds, otherwise empty.
- `datetime_raw`: raw OCR substring that was selected as the datetime candidate, otherwise empty.
- `ocr_text`: full recognized OCR text for the crop or image.
- `datetime_format`: the datetime format supplied by the user.
- `status`: one of `ok`, `ocr_failed`, `parse_failed`, or `unsupported`.
- `error`: error message for failed rows, otherwise empty.

Rows should be written even when a file fails, so batch runs are auditable and resumable by inspection.

## OCR Engine

Use the `ocrs` Rust library directly, targeting the current stable crate line (`0.12.x` at design time). The engine should be wrapped behind a small Serval module so CLI, media traversal, and CSV output are not coupled to `ocrs` APIs.

Implementation notes:

- Load OCR models once per command invocation, not once per media file.
- Use release builds for meaningful performance; `ocrs` and `rten` are very slow in debug builds.
- Pass `--allowed-chars` through to the OCR engine when provided. Leave the default recognition character set untouched when the option is absent.
- Keep model path and model acquisition explicit in the implementation plan. The design does not require auto-download in the first version.

## Media Processing

Images:

- Read the image directly.
- Apply optional relative crop.
- Run OCR on the resulting image.

Videos:

- Use FFmpeg to extract the first frame.
- Apply optional relative crop.
- Run OCR on the resulting frame.

The first-frame rule is deliberate. It is predictable across frame rates and avoids the old branch's hard-coded frame index behavior.

Crop handling:

- `--box` is relative, so it works across media with different resolutions.
- Invalid boxes should fail fast before processing the batch.
- Zero-area or out-of-range boxes are rejected with a clear error. Serval does not silently clamp user-supplied crop boxes.

## Datetime Extraction

Datetime parsing is strict-only in the first version.

- `--datetime-format` is required.
- Accept user-friendly tokens such as `YYYY-MM-DD HH:mm:ss` and translate them to `chrono` strptime tokens.
- Also accept native chrono formats that already contain `%` tokens.
- Extract likely datetime candidates from OCR text, apply limited OCR-specific cleanup, then parse only with the supplied format.
- If no candidate matches the supplied format, write `parse_failed`.
- Normalize successful parses to `yyyy-MM-dd HH:mm:ss`.

There is no fallback format guessing in the first version. This keeps behavior predictable for batches where all media share the same watermark template and date format.

OCR cleanup should stay conservative. It may map common confusions such as `O` to `0` or `l` to `1` inside datetime candidates, but it should not rewrite arbitrary OCR text or invent missing date parts.

## Error Handling

The command should continue processing after per-file failures.

Failure handling:

- Unsupported media or extraction failures produce a row with `status=unsupported` or `status=ocr_failed`.
- OCR engine failures produce `status=ocr_failed`.
- Datetime parsing failures produce `status=parse_failed` while preserving `ocr_text`.
- Invalid CLI parameters, model loading failures, or output directory creation failures are command-level errors and should stop before or at the point they occur.

## Test Strategy

Unit tests:

- Parse valid and invalid `--box` values.
- Convert user datetime formats to chrono formats.
- Parse datetime examples with the required user-supplied format.
- Reject impossible dates and malformed OCR candidates.

Integration-level tests where feasible:

- Verify directory traversal produces expected CSV rows for sample image files.
- Verify per-file failures do not abort the whole batch.

Manual verification:

- Run `serval ocr <sample-dir> --sample 5 --debug-crops --box ...`.
- Inspect `debug_crops`.
- Run a batch with `--datetime-format`.

## Out of Scope

- Updating XMP metadata directly from OCR output.
- Merging OCR output into `tags.csv`.
- Interactive visual crop selection.
- Training custom OCR models.
- Media-type filtering flags.
- Fallback datetime format guessing.
