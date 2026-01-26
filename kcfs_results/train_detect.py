import os
import torch
from datasets import load_from_disk
from transformers import (
    AutoTokenizer,
    AutoModelForSeq2SeqLM,
    Seq2SeqTrainingArguments,
    Seq2SeqTrainer,
    DataCollatorForSeq2Seq
)
from peft import LoraConfig, get_peft_model, TaskType, prepare_model_for_kbit_training

# --- 1. 配置路径与参数 ---
# 你的结构预训练模型路径 (Base Model)
MODEL_PATH = "/ssd_2t_1/wyq_workspace/genkubesect_structural_model"
# 你的数据集路径 (由 build_dataset.py 生成)
DATASET_PATH = "/home/wyq/GenKubeSec_Reproduce/kcfs_results/genkubesec_dataset"
# 最终 LoRA 模型保存路径
OUTPUT_DIR = "/ssd_2t_1/wyq_workspace/genkubesect_detection_model"

# 超参数 (参考 GenKubeSec 论文)
MAX_SOURCE_LEN = 512   # CodeT5p 上限
MAX_TARGET_LEN = 128   # 标签字符串通常不长
BATCH_SIZE = 8       # 根据显存调整 (4090/A100 可设 16-32, 显存小则 8)
NUM_EPOCHS = 5        # 微调通常 5-10 轮
LEARNING_RATE = 2e-4   # LoRA 常用学习率

def main():
    print(f"🚀 正在加载基础模型: {MODEL_PATH} ...")
    
    # --- 2. 加载 Tokenizer 和 模型 ---
    # 注意: CodeT5p 需要 trust_remote_code=True
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, trust_remote_code=True)
    
    # 加载 Seq2Seq 模型
    model = AutoModelForSeq2SeqLM.from_pretrained(
        MODEL_PATH,
        trust_remote_code=True,
        torch_dtype=torch.float16, # 使用 fp16 节省显存
        device_map="auto"          # 自动分配显卡
    )

    # --- 3. 配置 LoRA (Low-Rank Adaptation) ---
    # 论文参数: r=128, lora_alpha=256, dropout=0.125
    peft_config = LoraConfig(
        task_type=TaskType.SEQ_2_SEQ_LM, 
        inference_mode=False, 
        r=128, 
        lora_alpha=256, 
        lora_dropout=0.125,
        # CodeT5p (T5结构) 的 Attention 模块通常叫 'q', 'v'
        target_modules=["q", "v"] 
    )
    
    # 将模型转换为 PEFT 模型
    model = get_peft_model(model, peft_config)
    model.print_trainable_parameters() # 打印可训练参数量，确认 LoRA 生效

    # --- 4. 数据预处理 ---
    print(f"📂 正在加载数据集: {DATASET_PATH} ...")
    dataset = load_from_disk(DATASET_PATH)

    def preprocess_function(examples):
        # 输入: YAML 内容
        inputs = examples["source"]
        # 输出: 错误标签 (如 "Deployment+10, Service+52")
        targets = examples["target"]
        
        # Tokenize 输入
        model_inputs = tokenizer(
            inputs, 
            max_length=MAX_SOURCE_LEN, 
            padding="max_length", 
            truncation=True
        )

        # Tokenize 输出 (Labels)
        labels = tokenizer(
            targets, 
            max_length=MAX_TARGET_LEN, 
            padding="max_length", 
            truncation=True
        ).input_ids

        # 将 Padding 的 Label ID 设为 -100，以便在计算 Loss 时忽略
        labels_with_ignore_index = []
        for label_example in labels:
            label_example = [label if label != 0 else -100 for label in label_example]
            labels_with_ignore_index.append(label_example)
        
        model_inputs["labels"] = labels_with_ignore_index
        return model_inputs

    print("⚙️ 正在处理数据 (Tokenization)...")
    tokenized_datasets = dataset.map(
        preprocess_function,
        batched=True,
        remove_columns=dataset["train"].column_names, # 移除原始列，只保留 input_ids, labels
        num_proc=8
    )

    # --- 5. 配置训练参数 ---
    training_args = Seq2SeqTrainingArguments(
        output_dir=OUTPUT_DIR,
        per_device_train_batch_size=BATCH_SIZE,
        per_device_eval_batch_size=BATCH_SIZE,
        gradient_accumulation_steps=8,
        learning_rate=LEARNING_RATE,
        num_train_epochs=NUM_EPOCHS,
        weight_decay=0.01,

        eval_strategy="steps",
        save_strategy="steps",
        eval_steps=500,                   # 每 500 步评估一次
        save_steps=500,
        save_total_limit=3,
        metric_for_best_model="eval_loss",
        # evaluation_strategy="epoch",  # 每个 Epoch 评估一次
        # save_strategy="epoch",        # 每个 Epoch 保存一次
        # save_total_limit=2,           # 只保留最新的 2 个模型
        predict_with_generate=True,   # 评估时生成文本
        # GPU 0 是 3090，完美支持 bf16
        bf16=True,                        
        fp16=False,
        # fp16=True,                    # 开启混合精度
        logging_dir=f"{OUTPUT_DIR}/logs",
        logging_steps=100,
        load_best_model_at_end=True,  # 训练结束加载验证集表现最好的模型
        report_to="none"              # 不上传 WandB
    )

    # 数据整理器 (处理 Padding)
    data_collator = DataCollatorForSeq2Seq(
        tokenizer=tokenizer,
        model=model
    )

    # --- 6. 开始训练 ---
    trainer = Seq2SeqTrainer(
        model=model,
        args=training_args,
        train_dataset=tokenized_datasets["train"],
        eval_dataset=tokenized_datasets["validation"],
        data_collator=data_collator,
        tokenizer=tokenizer,
    )

    print("🔥 开始微调 (Fine-tuning)...")
    trainer.train()

    # --- 7. 保存最终模型 ---
    print(f"💾 保存模型到 {OUTPUT_DIR} ...")
    # 保存 adapter
    model.save_pretrained(OUTPUT_DIR)
    # 保存 tokenizer
    tokenizer.save_pretrained(OUTPUT_DIR)
    
    print("✅ 训练完成！")

if __name__ == "__main__":
    main()