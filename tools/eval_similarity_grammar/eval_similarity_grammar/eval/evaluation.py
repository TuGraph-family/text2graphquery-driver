import os
import sys
import argparse
import importlib
import json
import prettytable as pt
from evaluator.evaluator import Evaluator
from evaluator.similarity_evaluator import SimilarityEvaluator
from tqdm import tqdm

def evaluate(gold, predict, etype, impl,level="default"):
    # 修改日志文件名，加上 etype 后缀
    log_filename = f"eval_{etype}_{level}.log"
    log_path = f"{os.path.dirname(__file__)}/../output/logs/{log_filename}"
    log_file = open(log_path, "w", encoding="utf-8")
    log_lines = []
    with open(gold) as f:
        gseq_one = []
        db_id_list = []
        for l in f.readlines():
            gseq_one.append(l.strip())
            db_id_list.append(None)

    with open(predict) as f:
        pseq_one = []
        for l in f.readlines():
            if len(l.strip()) == 0:
                pseq_one.append("no out")
            else:
                pseq_one.append(l.strip())

    assert len(gseq_one) == len(pseq_one), "number of predicted queries and gold standard queries must equal"

    score_total = 0
    if etype == "similarity":
        evaluator = SimilarityEvaluator()
    elif etype == "grammar":
        model_path = f"evaluator.impl.{impl}.grammar_evaluator"
        m = importlib.import_module(model_path)
        GrammarEvaluator = getattr(m, "GrammarEvaluator")
        evaluator = GrammarEvaluator()
    elif etype == "execution":
        model_path = f"evaluator.impl.{impl}.execution_evaluator"
        m = importlib.import_module(model_path)
        ExecutionEvaluator = getattr(m, "ExecutionEvaluator")
        evaluator = ExecutionEvaluator()

    total = 0
    pbar = tqdm(range(len(gseq_one)), desc="Evaluating")
    for i in pbar:
        try:
            # --- 核心修复：自动探测参数个数 ---
            import inspect
            # 获取当前 evaluate 方法的参数列表
            sig = inspect.signature(evaluator.evaluate)
            params_count = len(sig.parameters)

            if params_count == 3:
                # 针对需要 (pred, gold, db_id) 的插件（比如你现在的 Cypher 插件）
                score = evaluator.evaluate(pseq_one[i], gseq_one[i], db_id_list[i])
            else:
                # 针对只需要 (pred, gold) 的插件（比如之前的 GQL 插件）
                score = evaluator.evaluate(pseq_one[i], gseq_one[i])
            
            # -------------------------------
        except Exception as e:
            # 如果还是崩了，打印出来具体原因，并给个 0 分保底
            print(f"Item {i} failed: {e}")
            score = 0
            
        score_total += max(score, 0)
        total += 1
        tmp_log = {"pred": pseq_one[i], "gold": gseq_one[i], "score": score}
        log_lines.append(tmp_log)
        pbar.update(1)

    # --- 核心：原始代码就是在这里把整个 JSON 扔进去的 ---
    json.dump(log_lines, log_file, ensure_ascii=False, indent=4)
    
    # !!! 必须加这一行，否则主程序读不到内容 !!!
    log_file.close() 

    tb = pt.PrettyTable()
    tb.field_names = ["Evaluation Type", "Total Count", "Accuracy"]
    accuracy = score_total / total if total > 0 else 0.0
    tb.add_row([etype, len(gseq_one), "{:.3f}".format(accuracy)])
    print(tb)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        type=str,
        help="the path to the input file",
        required=True,
    )
    parser.add_argument(
        "--gold", dest="gold", type=str, help="the path to the gold queries", default=""
    )
    parser.add_argument(
        "--etype",
        dest="etype",
        type=str,
        default="similarity",
        help="evaluation type, exec for test suite accuracy, match for the original exact set match accuracy",
        choices=("similarity", "grammar", "execution"),
    )
    parser.add_argument(
        "--impl",
        dest="impl",
        type=str,
        default="tugraph-db",
        help="implementation folder for grammar evaluator",
    )
    parser.add_argument("--level", dest="level", type=str, default="default")
    args = parser.parse_args()

    # Print args
    print(f"params as fllows \n {args}")

    # Second, evaluate the predicted GQL queries
    evaluate(args.gold, args.input, args.etype, args.impl, args.level)