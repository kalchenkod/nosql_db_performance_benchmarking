import os
from google.cloud import datastore


class Datastore:
    def init(self):
        credential_path = "#####"
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
        self.datastore_client = datastore.Client()
        self.name = 'datastore'

    def create(self, items):
        for item in items:
            review = datastore.Entity(key=self.datastore_client.key('Review', item['id']))
            item['text'] = item['text'][:int(len(item['text']) / 3)]
            review.update(item)
            self.datastore_client.put(review)

    def read(self):
        query = self.datastore_client.query(kind="Review")
        entities = list(query.fetch())
        return entities

    def update(self, items=None):
        query = self.datastore_client.query(kind='Review')
        entities = list(query.fetch())

        reviews = []
        for entity in entities:
            entity['user_id'] = entity['user_id'] + '!'
            reviews.append(entity)
        self.datastore_client.put_multi(entities)

    def delete(self):
        query = self.datastore_client.query(kind='Review')
        query.keys_only()
        entities = list(query.fetch())

        for entity in entities:
            self.datastore_client.delete(entity.key)
