# `tags.csv` Schema

`tags.csv` is the canonical editable CSV produced by `serval observe`.

## Canonical Header

```text
path,filename,media_type,datetime,latitude,longitude,species,individual,count,sex,bodypart,rating,custom,xmp_update,xmp_update_datetime,read_status,read_error
```

## Rules

- `serval observe` always writes the full canonical header in this order.
- Empty values are represented by empty CSV cells.
- Users may edit any column.
- Value validation remains the responsibility of the consuming subcommand.
- The canonical column name is `datetime`.
- `datetime_original` is accepted only as a legacy compatibility alias when reading older files.

## Column Notes

| Column | Meaning |
| --- | --- |
| `path` | Path to the media or XMP resource represented by the row. |
| `filename` | File name for review and manual editing. |
| `media_type` | Media type inferred from the underlying media path. For `*.xmp` sidecars, Serval strips the trailing `.xmp` before inferring the type. JPEG, PNG, MP4, and MOV use IANA-registered values. AVI currently uses the compatibility fallback `video/x-msvideo`. |
| `datetime` | Observation datetime used by capture-related workflows. |
| `latitude`, `longitude` | Coordinates read from metadata when available. |
| `species` | Species annotation for the row. |
| `individual` | Individual annotation for the row. |
| `count` | Count annotation. |
| `sex` | Sex annotation. |
| `bodypart` | Bodypart annotation. |
| `rating` | Rating value. |
| `custom` | Free-form user-maintained column. |
| `xmp_update` | Replacement tag value used by `serval xmp update` tag mode. |
| `xmp_update_datetime` | Replacement datetime used by `serval xmp update --datetime`. |
| `read_status` | `succeeded` for a readable resource, including one without labels. `failed` for a resource that could not be read. |
| `read_error` | Diagnostic for a failed read. Empty after success. |

One successful resource can produce multiple annotation rows. A failed resource produces one row with its path, filename, inferred media type and diagnostic. Statistics, capture and XMP edits skip failed rows. Older tables without these columns retain their original behavior. A file list may include failed paths for retry.

## Writing Back to XMP

Two commands write a table into the XMP sidecars:

- `serval xmp write <CSV>` writes the table as it is: for every file in the table, `species`, `individual`,
  `rating` and `datetime` become exactly the table's values (species and individuals collected from all of the
  file's rows). Empty cells leave a field unchanged. `--fields species,rating` limits the fields, `--dry-run`
  shows what would change, `--create-missing` creates missing sidecars as `xmp init` does.
- `serval xmp update <CSV>` changes old values into new ones (`xmp_update`, `xmp_update_datetime`) and refuses
  rows whose old value no longer matches the file.

## Non-Canonical Columns

The following are not part of the canonical base `tags.csv` schema:

- `subjects`
- `time_modified`
- `event_id`
- `deployment`

They may appear in debug, derived, or workflow-specific outputs, but they are not part of the base editable schema.
