#!/usr/bin/python3
import csv, json, sys
from geojson import Feature, FeatureCollection, Point

# Check usage
if len(sys.argv) != 3:
    print("Usage: python tsvscript.py input.tsv output.geojson")
    sys.exit(1)

tsv_file = sys.argv[1]
geojson_file = sys.argv[2]

features = []

with open(tsv_file, newline='') as csvfile:
    reader = csv.DictReader(csvfile, delimiter='\t') 

    for row in reader:
        try:
            props = {}
            # Geo coordinates: standard names
            if {'latitude', 'longitude', 'speed'}.issubset(row.keys()):
                lat = float(row['latitude'])
                lon = float(row['longitude'])
                speed = float(row['speed'])
                props['speed'] = int(speed)

                # Optional metadata
                for key in ['tstamp', 'trip_id', 'direction', 'service_key', 'weather']:
                    if key in row:
                        props[key] = row[key]

                feature = Feature(geometry=Point((lon, lat)), properties=props)
                features.append(feature)

            # Alternate x/y format
            elif {'x', 'y', 'speed'}.issubset(row.keys()):
                lat = float(row['y'])
                lon = float(row['x'])
                speed = float(row['speed'])
                props['speed'] = int(speed)

                feature = Feature(geometry=Point((lon, lat)), properties=props)
                features.append(feature)

            # Raw 4-column fallback (like data5.tsv)
            elif len(row) == 4:
                try:
                    lat = float(list(row.values())[0])
                    lon = float(list(row.values())[1])
                    speed = float(list(row.values())[2])
                    weather = list(row.values())[3]

                    props['speed'] = int(speed)
                    props['weather'] = weather

                    feature = Feature(geometry=Point((lon, lat)), properties=props)
                    features.append(feature)
                except (ValueError, IndexError):
                    continue


        except (ValueError, TypeError):
            continue  # skip bad rows


collection = FeatureCollection(features)

with open(geojson_file, "w") as f:
    json.dump(collection, f, indent=2)
