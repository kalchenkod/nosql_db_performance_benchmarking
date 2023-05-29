import time
import json
import psycopg2

from cosmosdb import CosmosDB
from datastore import Datastore
from dynamodb import DynamoDB


def test_crud_operations(db, data, iterations):
    test_results = []
    for i in range(iterations):
        print(f'Iteration: {i + 1}')
        crud_time_measurements = {'run_id': i + 1}

        # CREATE
        start_time = time.time()
        db.create(data)
        create_time = time.time() - start_time
        crud_time_measurements['CREATE'] = create_time

        # READ
        start_time = time.time()
        db.read()
        create_time = time.time() - start_time
        crud_time_measurements['READ'] = create_time

        # UPDATE
        start_time = time.time()
        db.update(data)
        create_time = time.time() - start_time
        crud_time_measurements['UPDATE'] = create_time

        # DELETE
        start_time = time.time()
        db.delete()
        create_time = time.time() - start_time
        crud_time_measurements['DELETE'] = create_time

        # Save results
        test_results.append(crud_time_measurements)
        print(f"{db.name}: {crud_time_measurements}")

    return test_results


def save_results_to_db(test_results, size, db_name):
    conn = psycopg2.connect(
        host="#####",
        database="#####",
        user="#####",
        password="#####")
    cursor = conn.cursor()

    cursor.execute(
        f'create table if not exists create_{size} (run_id integer, time_measure numeric(19, 10), db varchar(20))')
    cursor.execute(
        f'create table if not exists read_{size} (run_id integer, time_measure numeric(19, 10), db varchar(20))')
    cursor.execute(
        f'create table if not exists update_{size} (run_id integer, time_measure numeric(19, 10), db varchar(20))')
    cursor.execute(
        f'create table if not exists delete_{size} (run_id integer, time_measure numeric(19, 10), db varchar(20))')

    for result in test_results:
        cursor.execute(f'insert into create_{size} ("run_id", "time_measure", "db") '
                       f'values (%s, %s, %s)', (result["run_id"], result["CREATE"], db_name))

        cursor.execute(f'insert into read_{size} (run_id, time_measure, db) '
                       f'values (%s, %s, %s)', (result["run_id"], result["READ"], db_name))

        cursor.execute(f'insert into update_{size} (run_id, time_measure, db)'
                       f'values (%s, %s, %s)', (result["run_id"], result["UPDATE"], db_name))

        cursor.execute(f'insert into delete_{size} (run_id, time_measure, db)'
                       f'values (%s, %s, %s)', (result["run_id"], result["DELETE"], db_name))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    # Dataset load
    with open('data/dataset_10000.json') as f:
        dataset = json.load(f)

    # DBs connection
    databases = [CosmosDB(), DynamoDB(), Datastore()]

    for database in databases:
        # Test launching
        results = test_crud_operations(database, dataset, 1000)

        # Save results to postgres database
        save_results_to_db(results, 10000, database.name)
