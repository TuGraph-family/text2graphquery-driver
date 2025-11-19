import logging
import json
from neo4j import GraphDatabase
from pathlib import Path


class ExecutionEvaluator:
    def __init__(self, uri="bolt://localhost:7687", user="admin", password="73@TuGraph"):
        try:
            self.driver = GraphDatabase.driver(uri, auth=(user, password))
            self.driver.verify_connectivity()
            print(f"Connected to TuGraph at {uri}")
        except Exception as e:
            print(f"Failed to connect: {e}")
            self.driver = None

    def close(self):
        if self.driver:
            self.driver.close()

    # ------------------------------
    # 结果比较函数（与你原版完全一致）
    # ------------------------------
    def compare_results(self, res_gold, res_predict):

        def normalize(value):
            if isinstance(value, float):
                return round(value, 9)
            elif isinstance(value, (int, str, bool)) or value is None:
                return value
            elif hasattr(value, "isoformat"):
                return value.isoformat()
            elif hasattr(value, "total_seconds"):
                return str(value)
            elif isinstance(value, (list, tuple)):
                return tuple(normalize(v) for v in value)
            elif isinstance(value, dict):
                return tuple(sorted((k, normalize(v)) for k, v in value.items()))
            return str(value)

        def normalize_row(row):
            return tuple(normalize(v) for v in row.values())

        gold_set = {normalize_row(r) for r in res_gold}
        pred_set = {normalize_row(r) for r in res_predict}
        return gold_set == pred_set

    # ------------------------------
    # 执行单条评估
    # ------------------------------
    def evaluate(self, query_predict, query_gold, database="movie"):

        if not query_predict or not isinstance(query_predict, str):
            return 0, [], []

        if not self.driver:
            return -1, [], []

        # Gold
        try:
            with self.driver.session(database=database) as session:
                res_gold = session.run(query_gold).data()
        except Exception as e:
            logging.error(f"Gold query failed: {e}")
            return -1, [], []

        # Predict
        try:
            with self.driver.session(database=database) as session:
                res_predict = session.run(query_predict).data()
        except Exception as e:
            logging.error(f"Predict query failed: {e}")
            return 0, res_gold, []

        # Compare
        ok = 1 if self.compare_results(res_gold, res_predict) else 0
        return ok, res_gold, res_predict


# =======================================================================================
# Main 评测 Pipeline
# =======================================================================================
if __name__ == "__main__":

    input_path = r"E:\accuracy\test\第二轮\movie\movie_5levels.json"

    evaluator = ExecutionEvaluator()

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 要评测的字段
    LEVEL_FIELDS = [
        ("initial_query", "initial_results.json"),
        ("level_1_query", "level_1_results.json"),
        ("level_2_query", "level_2_results.json"),
        ("level_3_query", "level_3_results.json"),
        ("level_4_query", "level_4_results.json"),
    ]

    out_dir = Path("./execution_results")
    out_dir.mkdir(exist_ok=True)

    # 遍历每一层级
    for query_key, output_file in LEVEL_FIELDS:

        print(f"\n==============================")
        print(f" Evaluating: {query_key}")
        print(f"==============================")

        correct = 0
        results_dump = []

        for item in data:
            instance_id = item["instance_id"]
            gold_query = item["gql_query"]
            pred_query = item.get(query_key)

            score, gold_res, pred_res = evaluator.evaluate(pred_query, gold_query)

            correct += 1 if score == 1 else 0

            results_dump.append({
                "instance_id": instance_id,
                "gold_query": gold_query,
                "pred_query": pred_query,
                "score": score,
                "gold_result": gold_res,
                "pred_result": pred_res,
            })

        accuracy = correct / len(data)
        print(f"Accuracy ({query_key}) = {accuracy:.2%}")

        # 保存到文件
        with open(out_dir / output_file, "w", encoding="utf-8") as f:
            json.dump(results_dump, f, indent=2, ensure_ascii=False)

    evaluator.close()

    print("\n✨ All evaluation done! Results saved in /execution_results/")
