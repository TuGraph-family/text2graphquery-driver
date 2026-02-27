from .base_system import BaseLLMSystem

# ======================== Zero-Shot ========================

class CypherZeroShotSystem(BaseLLMSystem):
    def _get_messages(self, question, knowledge):
        specific_knowledge = knowledge if knowledge else "No specific external knowledge provided."
        system_content = (
            "You are an expert in graph query languages, specifically openCypher.\n"
            "Schema:\n"
            f"{self.schema_text}\n\n"
            "Domain knowledge:\n"
            f"{specific_knowledge}\n\n"
            "Task: Convert the user's natural language question into a openCypher query.\n"
            "Output: Return only the query string."
        )
        return [{"role": "system", "content": system_content}, {"role": "user", "content": question.strip()}]

class GQLZeroShotSystem(BaseLLMSystem):
    def _get_messages(self, question, knowledge):
        specific_knowledge = knowledge if knowledge else "No specific external knowledge provided."
        system_content = (
            "You are an expert in graph query languages, specifically ISO GQL (ISO/IEC 39075).\n"
            "Schema (DDL):\n"
            f"{self.schema_text}\n\n"
            "Domain knowledge:\n"
            f"{specific_knowledge}\n\n"
            "Task: Convert the user's natural language question into a ISO GQL query.\n"
            "Output: Return only the query string."
        )
        return [{"role": "system", "content": system_content}, {"role": "user", "content": question.strip()}]

class SQLZeroShotSystem(BaseLLMSystem):
    def _get_messages(self, question, knowledge):
        specific_knowledge = knowledge if knowledge else "No specific external knowledge provided."
        system_content = (
            "You are an expert in relational query languages, specifically SQL.\n"
            "Schema:\n"
            f"{self.schema_text}\n\n"
            "Domain knowledge:\n"
            f"{specific_knowledge}\n\n"
            "Task: Convert the user's natural language question into a SQL query.\n"
            "Output: Return only the query string."
        )
        return [{"role": "system", "content": system_content}, {"role": "user", "content": question.strip()}]


# ======================== Few-Shot ========================

class CypherFewShotSystem(BaseLLMSystem):
    def _get_messages(self, question, knowledge):
        specific_knowledge = knowledge if knowledge else "No specific external knowledge provided."
        system_content = (
            "You are an expert in graph query languages, specifically openCypher.\n"
            "The database schema is as follows:\n"
            f"{self.schema_text}\n\n"       
            "Domain Knowledge:\n"
            f"{specific_knowledge}\n\n"        
            "The user's question and corresponding output examples are as follows:\n\n"
            "Example 1\n"
            "Question: Which characters have a path to \"Catelyn-Stark\" in the interaction network with a maximum of 3 hops?\n"
            "Output: MATCH (c:Character)-[:INTERACTS*1..3]->(target:Character {name: 'Catelyn-Stark'}) RETURN DISTINCT c.name\n\n"
            "Example 2\n"
            "Question: How many people have directed more than two movies?\n"
            "Output: MATCH (p:Person)-[:DIRECTED]->(m:Movie) WITH p, count(m) AS moviesDirected WHERE moviesDirected > 2 RETURN count(p) AS directorsCount\n\n"
            "Example 3\n"
            "Question: List the top 5 movies with the most production companies involved.\n"
            "Output: MATCH (m:Movie)-[:PRODUCED_BY]->(pc:ProductionCompany) WITH m, COUNT(pc) AS productionCompanyCount ORDER BY productionCompanyCount DESC LIMIT 5 RETURN m.title AS MovieTitle, productionCompanyCount"
        )
        return [{"role": "system", "content": system_content}, {"role": "user", "content": question.strip()}]

class GQLFewShotSystem(BaseLLMSystem):
    def _get_messages(self, question, knowledge):
        specific_knowledge = knowledge if knowledge else "No specific external knowledge provided."
        system_content = (
            "You are an expert in graph query languages, specifically ISO GQL (ISO/IEC 39075).\n"
            "The database schema is as follows:\n"
            f"{self.schema_text}\n\n"
            "Domain Knowledge:\n"
            f"{specific_knowledge}\n\n"
            "Task: Convert the user's natural language question into a ISO GQL query.\n"
            "Output: Return only the query string.\n\n"
            "The user's question and corresponding output examples are as follows:\n\n"
            "Example 1\n"
            "Question: Please list the Asian populations of all the residential areas with the bad alias \"URB San Joaquin\".\n"
            "Output: MATCH (t1:zip_data)<-[zip_code:ZIP_CODE]-(t2:avoid) WHERE t2.bad_alias = 'URB San Joaquin' RETURN sum(t1.asian_population)\n\n"
            "Example 2\n"
            "Question: What is the country and state of the city named Dalton?\n"
            "Output: MATCH (t1:state)<-[t2:country]-(t3:zip_data) WHERE t3.city = 'Dalton' RETURN t2.county\n\n"
            "Example 3\n"
            "Question: How many cities does congressman Pierluisi Pedro represent?\n"
            "Output: MATCH (t1:zip_data)<-[t2:zip_congress]-(t3:congress) WHERE (t3.first_name = 'Pierluisi' AND t3.last_name = 'Pedro') RETURN count(DISTINCT t1.city)"
        )
        return [{"role": "system", "content": system_content}, {"role": "user", "content": question.strip()}]

class SQLFewShotSystem(BaseLLMSystem):
    def _get_messages(self, question, knowledge):
        specific_knowledge = knowledge if knowledge else "No specific external knowledge provided."
        system_content = (
            "You are an expert in relational query languages, specifically SQL.\n"
            "The database schema is as follows:\n"
            f"{self.schema_text}\n\n"
            "Domain Knowledge:\n"
            f"{specific_knowledge}\n\n"
            "Task: Convert the user's natural language question into a SQL query.\n"
            "Output: Return only the query string.\n\n"
            "The user's question and corresponding output examples are as follows:\n\n"
            "Example 1\n"
            "Question: Please list the Asian populations of all the residential areas with the bad alias \"URB San Joaquin\".\n"
            "Output: SELECT SUM(T1.asian_population) FROM zip_data AS T1 INNER JOIN avoid AS T2 ON T1.zip_code = T2.zip_code WHERE T2.bad_alias = 'URB San Joaquin'\n\n"
            "Example 2\n"
            "Question: What is the country and state of the city named Dalton?\n"
            "Output: SELECT T2.county FROM state AS T1 INNER JOIN country AS T2 ON T1.abbreviation = T2.state INNER JOIN zip_data AS T3 ON T2.zip_code = T3.zip_code WHERE T3.city = 'Dalton' GROUP BY T2.county\n\n"
            "Example 3\n"
            "Question: How many cities does congressman Pierluisi Pedro represent?\n"
            "Output: SELECT COUNT(DISTINCT T1.city) FROM zip_data AS T1 INNER JOIN zip_congress AS T2 ON T1.zip_code = T2.zip_code INNER JOIN congress AS T3 ON T2.district = T3.cognress_rep_id WHERE T3.first_name = 'Pierluisi' AND T3.last_name = 'Pedro'"
        )
        return [{"role": "system", "content": system_content}, {"role": "user", "content": question.strip()}]