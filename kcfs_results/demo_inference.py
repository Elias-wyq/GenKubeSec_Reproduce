import torch
from peft import PeftModel
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

# --- 配置路径 ---
BASE_MODEL = "/ssd_2t_1/wyq_workspace/genkubesect_structural_model"
LORA_MODEL = "/ssd_2t_1/wyq_workspace/genkubesect_detection_model" # 刚才训练完的路径

def main():
    print("正在加载模型 (这可能需要几分钟)...")
    # 1. 加载 Tokenizer
    tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL, trust_remote_code=True)
    
    # 2. 加载基础模型
    base_model = AutoModelForSeq2SeqLM.from_pretrained(
        BASE_MODEL,
        trust_remote_code=True,
        torch_dtype=torch.float16,
        device_map="auto"
    )
    
    # 3. 加载刚才训练好的 LoRA
    model = PeftModel.from_pretrained(base_model, LORA_MODEL)
    model.eval()

    # --- 测试案例 1: 一个明显有问题的 Deployment ---
    # 问题: 使用了 latest 标签，且没有限制 CPU/内存
    bad_yaml = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-deployment
spec:
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:latest
        ports:
        - containerPort: 80
    """

    print("\n" + "="*30)
    print("测试案例: 输入一段有缺陷的 YAML")
    print("="*30)

    # 推理
    inputs = tokenizer(bad_yaml, return_tensors="pt", max_length=512, truncation=True).to(model.device)
    
    with torch.no_grad():
        outputs = model.generate(
            input_ids=inputs["input_ids"],
            max_new_tokens=128,
            num_beams=5, # 使用 Beam Search 效果更好
            early_stopping=True
        )
    
    result = tokenizer.decode(outputs[0], skip_special_tokens=True)
    print(f"\n[模型判定结果]:\n{result}")

if __name__ == "__main__":
    main()