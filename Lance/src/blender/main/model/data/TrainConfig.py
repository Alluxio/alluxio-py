from dataclasses import dataclass


@dataclass
class TrainConfig:
    model_id: str
    max_length: int = 256
    lr: float = 5e-5
    batch_size: int = 16
    epochs: int = 1
    warmup_ratio: float = 0.1
    weight_decay: float = 0.01
    grad_accum_steps: int = 1
    fp16: bool = False
