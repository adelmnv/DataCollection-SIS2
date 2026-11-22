Earthquakes table

| Column | Type | Description |
|---|---|---|
| id | TEXT (PK) | Unique USGS event id |
| time | TEXT | Event time, UTC |
| mag | REAL | Magnitude |
| mag_type | TEXT | Magnitude type (mww, mb, ml, ...) |
| place | TEXT | Text description of the location |
| region | TEXT | Last part of place (country or region) |
| depth_km | REAL | Depth in km |
| latitude | REAL | Epicenter latitude, degrees |
| longitude | REAL | Epicenter longitude, degrees |
| sig | INTEGER | USGS significance score |
| nst | REAL | Number of stations used |
| gap | REAL | Azimuthal gap, degrees |
| dmin | REAL | Distance to nearest station |
| rms | REAL | Root mean square of travel time residuals |
| status | TEXT | automatic or reviewed |
| event_type | TEXT | Event type (earthquake, ...) |
| net | TEXT | Data contributor network |
| alert | TEXT | PAGER alert level (green, yellow, orange, red, none) |
| tsunami | INTEGER | 1 if tsunami flag is set, else 0 |
| depth_category | TEXT | shallow (<70 km), intermediate (<300 km), deep |
| mag_class | TEXT | M<5, M5-5.9, M6-6.9, M7+ |
| energy_log10_joules | REAL | 1.5 * mag + 4.8 |
| mag_zscore | REAL | Standardized magnitude |