from azure.cosmos import CosmosClient


class CosmosDB:
    def init(self):
        url = "#####"
        key = '#####'
        self.cosmos_client = CosmosClient(url, credential=key)
        self.name = 'cosmosdb'

    def create(self, items):
        database_name = 'reviews_db'
        container_name = 'reviews_container'
        database = self.cosmos_client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        for item in items:
            container.upsert_item(item)

    def read(self):
        database_name = 'reviews_db'
        container_name = 'reviews_container'
        database = self.cosmos_client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        for item in container.query_items(
                query='SELECT * FROM reviews_container',
                enable_cross_partition_query=True):
            pass

    def update(self, items):
        database_name = 'reviews_db'
        container_name = 'reviews_container'
        database = self.cosmos_client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        for item in items:
            container.upsert_item(item)

    def delete(self):
        database_name = 'reviews_db'
        container_name = 'reviews_container'
        database = self.cosmos_client.get_database_client(database_name)
        container = database.get_container_client(container_name)
        for item in container.query_items(
                query='SELECT * FROM reviews_container',
                enable_cross_partition_query=True):
            container.delete_item(item, partition_key=item["stars"])
