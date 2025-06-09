import json
import csv
import sys
import duckdb
import os

DB_PATH = os.getenv('DB_PATH', 'horsies.db')

def process_races(data):
    races = []
    for region, courses in data.items():
        for course, times in courses.items():
            for off_time, details in times.items():
                race = {
                    'region': region,
                    'course': details['course'],
                    'course_id': details['course_id'],
                    'race_id': details['race_id'],
                    'date': details['date'],
                    'off_time': details['off_time'],
                    'race_name': details['race_name'],
                    'distance_round': details['distance_round'],
                    'distance': details['distance'],
                    'distance_f': details['distance_f'],
                    'race_class': details['race_class'],
                    'type': details['type'],
                    'age_band': details['age_band'],
                    'rating_band': details['rating_band'],
                    'prize': details['prize'],
                    'field_size': details['field_size'],
                    'going_detailed': details.get('going_detailed',''),
                    'rail_movements': details['rail_movements'],
                    'stalls': details['stalls'],
                    'weather': details['weather'],
                    'going': details['going'],
                    'surface': details['surface']
                }
                races.append(race)
    return races

def process_runners(data):
    runners = []
    for region, courses in data.items():
        for course, times in courses.items():
            for off_time, details in times.items():
                for runner in details['runners']:
                    runner_record = {
                        'race_id': details['race_id'],
                        'horse_id': runner['horse_id'],
                        'name': runner['name'],
                        'age': runner['age'],
                        'sex': runner['sex'],
                        'colour': runner['colour'],
                        'region': runner['region'],
                        'breeder': runner['breeder'],
                        'dam': runner['dam'],
                        'dam_region': runner['dam_region'],
                        'sire': runner['sire'],
                        'sire_region': runner['sire_region'],
                        'grandsire': runner['grandsire'],
                        'damsire': runner['damsire'],
                        'damsire_region': runner['damsire_region'],
                        'trainer': runner['trainer'],
                        'trainer_id': runner['trainer_id'],
                        'trainer_location': runner['trainer_location'],
                        'owner': runner['owner'],
                        'number': runner['number'],
                        'draw': runner['draw'],
                        'headgear': runner['headgear'],
                        'lbs': runner['lbs'],
                        'ofr': runner['ofr'],
                        'rpr': runner['rpr'],
                        'ts': runner['ts'],
                        'jockey': runner['jockey'],
                        'jockey_id': runner['jockey_id'],
                        'last_run': runner['last_run'],
                        'form': runner['form'],
                        'trainer_rtf': runner['trainer_rtf']
                    }
                    runners.append(runner_record)
    return runners

def save_to_csv(data, filename, fieldnames):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def load_to_duckdb(csv_file, table_name):
    conn = duckdb.connect(DB_PATH)
    conn.execute(f"INSERT INTO {table_name} SELECT * FROM read_csv_auto('{csv_file}')")
    conn.close()

def create_tables_from_csv():
    conn = duckdb.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS races AS SELECT * FROM read_csv_auto('races.csv');")
    conn.execute("CREATE TABLE IF NOT EXISTS runners AS SELECT * FROM read_csv_auto('runners.csv');")
    conn.close()

def main(date):
    # Load JSON data
    with open(f'racecards/{date}.json', 'r') as f:
        data = json.load(f)

    # Process data
    races = process_races(data)
    runners = process_runners(data)

    # Define fieldnames for CSV files
    race_fieldnames = ['region', 'course', 'course_id', 'race_id', 'date', 'off_time', 'race_name', 'distance_round', 'distance', 'distance_f', 'race_class', 'type', 'age_band', 'rating_band', 'prize', 'field_size', 'going_detailed', 'rail_movements', 'stalls', 'weather', 'going', 'surface']
    runner_fieldnames = ['race_id', 'horse_id', 'name', 'age', 'sex', 'colour', 'region', 'breeder', 'dam', 'dam_region', 'sire', 'sire_region', 'grandsire', 'damsire', 'damsire_region', 'trainer', 'trainer_id', 'trainer_location', 'owner', 'number', 'draw', 'headgear', 'lbs', 'ofr', 'rpr', 'ts', 'jockey', 'jockey_id', 'last_run', 'form', 'trainer_rtf']

    # Save data to CSV files
    save_to_csv(races, 'races.csv', race_fieldnames)
    save_to_csv(runners, 'runners.csv', runner_fieldnames)

    # Create tables from CSV if not exist
    create_tables_from_csv()

    # Load data into DuckDB
    load_to_duckdb('races.csv', 'races')
    load_to_duckdb('runners.csv', 'runners')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Usage: racecard_process.py <date>')
        sys.exit(1)
    main(sys.argv[1])