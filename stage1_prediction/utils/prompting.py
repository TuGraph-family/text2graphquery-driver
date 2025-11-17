from .global_schema import SCHEMA_TEXT

def build_prompt(nl_question: str):
    return [
        {
            "role": "system",
            "content": (
                "You are an expert in graph query languages.\n"
                "The database schema is as follows:\n"
                f"{SCHEMA_TEXT}\n\n"
                "Your task: Given a natural language question, output ONLY one query:\n"
                "Cypher (for Neo4j)\n\n"
                "Requirements:\n"
                "- Use the schema exactly (labels, properties, edge types).\n"
                "- Maintain the exact relationship types and directions.\n"
                "- Preserve all temporal constraints.\n"
                "- Use DISTINCT when necessary.\n"
                "- For path length, use length(p)-1 if matching multi-hop paths.\n"
                "- Do not merge different edge types unless explicitly required.\n"
                "- Output must be plain query only, no comments, no explanation.\n"
            )
        },
        {"role": "user", "content": nl_question.strip()}
    ]
