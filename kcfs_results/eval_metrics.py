import torch
from peft import PeftModel
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer
from datasets import load_from_disk
from tqdm import tqdm

# --- 配置 ---
BASE_MODEL = "/ssd_2t_1/wyq_workspace/genkubesect_structural_model"
LORA_MODEL = "/ssd_2t_1/wyq_workspace/genkubesect_detection_model"
DATASET_PATH = "/home/wyq/GenKubeSec_Reproduce/kcfs_results/genkubesec_dataset"
BATCH_SIZE = 16 # 显存够大可以开大

def parse_labels(label_str):
    """将字符串 'Deployment+10, Service+52' 解析为集合 {'Deployment+10', 'Service+52'}"""
    if not label_str or label_str.strip() == "":
        return set()
    return set([x.strip() for x in label_str.split(',')])

def calculate_metrics(predictions, references):
    total_tp = 0
    total_fp = 0
    total_fn = 0
    
    for pred_str, ref_str in zip(predictions, references):
        pred_set = parse_labels(pred_str)
        ref_set = parse_labels(ref_str)
        
        # True Positives: 预测对的
        tp = len(pred_set.intersection(ref_set))
        # False Positives: 预测了但实际没有的 (误报)
        fp = len(pred_set - ref_set)
        # False Negatives: 实际有但没预测出来的 (漏报)
        fn = len(ref_set - pred_set)
        
        total_tp += tp
        total_fp += fp
        total_fn += fn
        
    precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0.0
    recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0.0
    f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
    
    return precision, recall, f1

def main():
    # 1. 加载模型
    print("正在加载模型...")
    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL, trust_remote_code=True)
    base_model = AutoModelForSeq2SeqLM.from_pretrained(
        BASE_MODEL, trust_remote_code=True, torch_dtype=torch.float16, device_map="auto"
    )
    model = PeftModel.from_pretrained(base_model, LORA_MODEL)
    model.eval()

    # 2. 加载测试集
    print("正在加载测试集...")
    dataset = load_from_disk(DATASET_PATH)
    test_data = dataset["test"] # 只使用测试集
    
    print(f"测试集大小: {len(test_data)}")
    
    # 3. 批量推理
    predictions = []
    references = test_data["target"] # 真实标签
    inputs = test_data["source"]     # 输入 YAML

    print("开始推理评估...")
    # 手动 Batch 处理
    for i in tqdm(range(0, len(inputs), BATCH_SIZE)):
        batch_inputs = inputs[i : i + BATCH_SIZE]
        
        # Tokenize
        model_inputs = tokenizer(
            batch_inputs, 
            max_length=512, 
            padding=True, 
            truncation=True, 
            return_tensors="pt"
        ).to(model.device)
        
        # Generate
        with torch.no_grad():
            outputs = model.generate(
                input_ids=model_inputs["input_ids"],
                attention_mask=model_inputs["attention_mask"],
                max_new_tokens=128,
                num_beams=3 # 稍微降低 beam 加速评估
            )
        
        # Decode
        batch_preds = tokenizer.batch_decode(outputs, skip_special_tokens=True)
        predictions.extend(batch_preds)

    # 4. 计算指标
    precision, recall, f1 = calculate_metrics(predictions, references)

    print("\n" + "="*30)
    print("📊 最终评估结果 (Test Set)")
    print("="*30)
    print(f"Precision (精确率): {precision:.4f}")
    print(f"Recall    (召回率): {recall:.4f}")
    print(f"F1 Score  (综合分): {f1:.4f}")
    print("="*30)

if __name__ == "__main__":
    main()