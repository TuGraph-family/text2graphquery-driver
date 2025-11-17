import time
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI
from .prompting import build_prompt

LEVEL_FIELDS = [
    ("initial_nl", "initial_query"),
    ("level_1", "level_1_query"),
    ("level_2", "level_2_query"),
    ("level_3", "level_3_query"),
    ("level_4", "level_4_query")
]

def call_single_instance(client, instance_id, question, model, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            completion = client.chat.completions.create(
                model=model,
                messages=build_prompt(question),
                extra_body={"enable_thinking": False},
                timeout=timeout
            )
            return completion.choices[0].message.content.strip()
        except Exception:
            if attempt == max_retries:
                return None
            time.sleep(1)

def predict_all_levels(data, api_key, base_url, model="qwen-plus", max_workers=5):

    def process_record(item):
        local_client = OpenAI(api_key=api_key, base_url=base_url)
        result = item.copy()
        instance_id = item["instance_id"]

        for nl_field, query_field in LEVEL_FIELDS:
            question = item.get(nl_field)
            if not question:
                result[query_field] = None
                continue

            cypher = call_single_instance(
                local_client,
                instance_id,
                question,
                model
            )
            result[query_field] = cypher

        return result

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(process_record, item) for item in data]

        for f in tqdm(as_completed(futures), total=len(futures), desc="Predicting"):
            results.append(f.result())

    results.sort(key=lambda x: int(x["instance_id"].split("_")[-1]))
    return results
