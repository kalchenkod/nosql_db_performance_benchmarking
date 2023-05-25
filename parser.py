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
        print(crud_time_measurements)

    return test_results


def save_results_to_db(test_results, size, db_name):
    conn = psycopg2.connect(
        host="#####",
        database="#####",
        user="#####",
        password="#####")
    cursor = conn.cursor()

    for result in test_results:
        cursor.execute(f'insert into {db_name}_create_{size} values ({result["run_id"]}, {result["CREATE"]})')
        cursor.execute(f'insert into {db_name}_read_{size} values ({result["run_id"]}, {result["READ"]})')
        cursor.execute(f'insert into {db_name}_update_{size} values ({result["run_id"]}, {result["UPDATE"]})')
        cursor.execute(f'insert into {db_name}_delete_{size} values ({result["run_id"]}, {result["DELETE"]})')

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
        results = test_crud_operations(database, dataset, iterations=1000)

        # Save results to postgres database
        save_results_to_db(results, 10000, database.name)

