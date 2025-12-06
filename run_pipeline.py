import json
import argparse
import os
import sys
from impl.text2graph_system.qwen_zeroshot_system import QwenZeroshotSystem
from impl.db_driver.tugraph_driver import TuGraphAdapter
from impl.evaluation.metrics import ExecutionAccuracy, GoogleBleu, ExternalMetric
from impl.text2graph_system.utils import clean_query

class PipelineRunner:
    """
    Executor responsible for chaining the prediction and evaluation workflows of the Text2Graph system.
    """
    def __init__(self, config_path):
        self.config_path = config_path
        self.cfg = self._load_config(config_path)
        self.db_driver = None
        self.results = [] # Used for sharing data between prediction and evaluation phases

    def _load_config(self, path):
        print(f"Loading configuration from {path}...")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _init_db_driver(self):
        """Initialize database connection"""
        # EA (Execution Accuracy) is now enabled
        eval_cfg = self.cfg["evaluation"]
        print(f"Connecting to TuGraph ({eval_cfg['db_uri']})...")
        self.db_driver = TuGraphAdapter(eval_cfg["db_uri"], eval_cfg["db_user"], eval_cfg["db_pass"])
        self.db_driver.connect()

    def run_prediction_phase(self):
        """Execute prediction phase logic"""
        data_path = self.cfg["data"]["input_path"]
        output_path = self.cfg["data"]["output_path"]

        if self.cfg["pipeline"]["run_prediction"]:
            print(f"Loading raw data from {data_path}...")
            with open(data_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
            
            print("Initializing Text2Graph System...")
            system = QwenZeroshotSystem(self.cfg["prediction"])
            
            print("Running Prediction Batch...")
            self.results = system.predict_batch(raw_data)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            print(f"Predictions saved to {output_path}")
        else:
            print(f"Skipping prediction. Loading existing results from {output_path}...")
            if not os.path.exists(output_path):
                print(f"Error: Output file {output_path} not found. Cannot evaluate.")
                sys.exit(1)
                
            with open(output_path, "r", encoding="utf-8") as f:
                self.results = json.load(f)

    def run_evaluation_phase(self):
        """Execute evaluation phase logic"""
        if not self.cfg["pipeline"]["run_evaluation"]:
            return

        print("\nStarting Evaluation...")
        eval_cfg = self.cfg["evaluation"]

        # 1. Initialize metrics
        # EA is enabled, so we initialize ExecutionAccuracy using self.db_driver
        ea_metric = ExecutionAccuracy(self.db_driver)
        
        bleu_metric = GoogleBleu()
        ext_metric = ExternalMetric(eval_cfg["dbgpt_root"])
        
        levels = self.cfg["prediction"]["level_fields"]

        # 2. Iterate through different difficulty levels for evaluation
        for _, query_key in levels:
            # We now pass ea_metric to the evaluation function
            self._evaluate_single_level(query_key, ea_metric, bleu_metric, ext_metric)

    def _evaluate_single_level(self, query_key, ea_metric, bleu_metric, ext_metric):
        """Evaluate a single difficulty level and save detailed results"""
        print(f"\n{'='*40}")
        print(f"Evaluating Level: {query_key}")
        print(f"{'='*40}")
        
        preds = []
        golds = []
        
        # Data cleaning and preparation
        for item in self.results:
            raw_p = item.get(query_key, "")
            raw_g = item.get("gql_query", "")
            p = clean_query(raw_p)
            g = clean_query(raw_g)
            preds.append(p)
            golds.append(g)
        
        # --- Metric Calculation ---
        print("Calculating Execution Accuracy...")
        # Computing EA
        ea = ea_metric.compute(preds, golds, db_ids=[])
        
        print("Calculating Google BLEU...")
        bleu = bleu_metric.compute(preds, golds)
        
        print("Calculating Grammar & Similarity...")
        ext_res = ext_metric.compute(preds, golds)
        
        # --- Print Summary ---
        print(f"\nResults for {query_key}:")
        print(f"  - Samples    : {len(preds)}")
        print(f"  - EA (Acc)   : {ea:.2%}")
        print(f"  - Grammar    : {ext_res['Grammar']:.2%}")
        print(f"  - Similarity : {ext_res['Similarity']:.4f}")
        print(f"  - BLEU       : {bleu if isinstance(bleu, str) else f'{bleu:.4f}'}")

        # --- Save Detailed Results ---
        # Pass 'ea' to be saved
        self._save_detailed_results(query_key, preds, golds, ea, bleu, ext_res)

    def _save_detailed_results(self, query_key, preds, golds, ea, bleu, ext_res):
        """Save evaluation details to file"""
        # Fixed path separator for cross-platform compatibility
        output_dir = os.path.join("evaluation_detail", "execution_results")
        os.makedirs(output_dir, exist_ok=True)

        detailed_records = []
        for i, item in enumerate(self.results):
            record = {
                "instance_id": item.get("id", i),
                "gold_query": golds[i],
                "pred_query": item.get(query_key, ""),
                "cleaned_pred": preds[i],
                "metrics": {
                    "accuracy": ea,  # EA is now included
                    "grammar": ext_res["Grammar"],
                    "similarity": ext_res["Similarity"],
                    "google_bleu": float(bleu) if not isinstance(bleu, str) else bleu
                },
                "gold_result": None,
                "pred_result": None
            }
            detailed_records.append(record)

        save_path = os.path.join(output_dir, f"{query_key}_results.json")
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(detailed_records, f, indent=2, ensure_ascii=False)
        print(f"Detailed results saved → {save_path}")

    def cleanup(self):
        """Resource cleanup"""
        if self.db_driver:
            try:
                self.db_driver.close()
                print("\nDatabase connection closed.")
            except Exception as e:
                print(f"Error closing database: {e}")

    def run(self):
        """Main entry point method"""
        try:
            # 1. Initialize resources (including DB)
            self._init_db_driver()

            # 2. Run phases
            self.run_prediction_phase()
            self.run_evaluation_phase()

        finally:
            self.cleanup()
            print("\nEvaluation Finished.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="experiment/test_config.json", help="Path to config file")
    args = parser.parse_args()

    # Instantiate and run
    runner = PipelineRunner(args.config)
    runner.run()

if __name__ == "__main__":
    main()