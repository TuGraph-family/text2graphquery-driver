import logging
from neo4j import GraphDatabase
from driver.evaluation import DatabaseDriver

class TuGraphAdapter(DatabaseDriver):
    """
    TuGraph Database Adapter
    """
    def __init__(self, uri, user, password):
        self.uri = uri
        self.auth = (user, password)
        self.driver = None

    def connect(self):
        try:
            # The default TuGraph port is usually 7687 (Bolt) as well.
            self.driver = GraphDatabase.driver(self.uri, auth=self.auth)
            self.driver.verify_connectivity()
            print(f"Connected to TuGraph at {self.uri}")
        except Exception as e:
            print(f"Failed to connect to TuGraph: {e}")
            self.driver = None

    def query(self, cypher: str, db_name: str = "default") -> list:
        """
        Executes a Cypher query against the specified graph in TuGraph.
        """
        if not self.driver:
            return None
        
        try:
            with self.driver.session(database=db_name) as session:
                result = session.run(cypher).data()
                return result
        except Exception as e:
            return None

    def close(self):
        if self.driver:
            self.driver.close()