# Serval

A command line tool for camera trap data wrangling in the [Maze](https://wsyxbcl.gitlab.io/ptilopsis) team.

# Build

```bash
cargo build
```

# Installation

```
cargo install maze-serval
```

# License
GPL-3.0
### Extract whole events

Use the events CSV from `serval capture --event`. Select one or more species
or event IDs by repeating `-v`. Each matching event gets an `event_<id>` folder
containing every member and its available XMP sidecar. `--keep-dirs` controls the
source folders preserved inside each event folder.

```sh
serval extract events_species_30m_LR.csv --unit event -f species -v Fox -v Pika --keep-dirs 0 -o fox-and-pika
serval extract events_species_30m_LR.csv --unit event -f event -v 17 --keep-dirs 1 -o chosen-events
```
