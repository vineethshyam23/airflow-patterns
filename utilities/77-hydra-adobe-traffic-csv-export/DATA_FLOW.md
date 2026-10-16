# Data flow: Hydra Adobe traffic CSV publish

## Window

Default window is **day-before-yesterday through yesterday** inclusive
(two calendar dates). Up to **10 CSVs** per run (5 tables × 2 dates).

## Stages

1. **Retention (`delete_old_files`)**  
   List publish-bucket objects. For each known metric prefix, parse
   `{prefix}_{date}.csv` (ISO or unpadded). Delete if file date is
   older than `END_DATE − 60 days`.

2. **Extract (`extract_and_upload_csv_files`)**  
   For each table × date:
   - `SELECT * FROM refined.{table} WHERE DATE(date) = '{date}'`
   - Reformat `date` column to `d-m-yyyy` (no zero-pad) for consumers
   - Map `deviceType` on visits-by-device (`tv` → Television, etc.)
   - Write CSV to Composer `data/site-traffic/{short}_{YYYY-MM-DD}.csv`

3. **Overwrite prep (`delete_target_files`)**  
   Delete publish-bucket objects whose names match the expected
   filenames for this window (both padded and unpadded date stems).

4. **Publish (`copy_files_to_targets`)**  
   Copy successful staging objects to the publish bucket **root**
   (filename only — no folder).

5. **Summaries**  
   Log copy and extract counts (success / no_data / error) for on-call.

6. **Staging cleanup (`cleanup_source_files`)**  
   Delete Composer staging objects only when the matching copy
   reported `success`. Failed copies stay for retry.

## Naming

| Refined table | CSV stem |
|---------------|----------|
| `site_traffic_mailphoneclicks` | `mailphoneclicks` |
| `site_traffic_openingtimes` | `openingtimes` |
| `site_traffic_traffic` | `traffic` |
| `site_traffic_visitsbydevice` | `visitsbydevice` |
| `site_traffic_visitsbylocale` | `visitsbylocale` |

Filename date is always `YYYY-MM-DD`. Cell `date` values use
`d-m-yyyy` without zero-padding — intentional consumer contract, not
a bug.

## Downstream

External Hydra / web traffic reporting jobs read the publish bucket
root. Upstream ownership of `site_traffic_*` sits with the Adobe →
refined mart pipelines (see patterns 49 / 51 for hit land; mart SQL
is out of scope here).
