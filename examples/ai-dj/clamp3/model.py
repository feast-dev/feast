"""Load CLaMP 3 once and embed text or MERT window features.

Mirrors vendor/clamp3/code/extract_clamp3.py (the `--get_global` path), so
vectors match the official tool.
"""

import os
import sys
from functools import lru_cache
from pathlib import Path

os.environ.setdefault("PYTORCH_ENABLE_MPS_FALLBACK", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "vendor" / "clamp3" / "code"))

import numpy as np  # noqa: E402
import torch  # noqa: E402
from config import (  # noqa: E402
    AUDIO_HIDDEN_SIZE,
    AUDIO_NUM_LAYERS,
    CLAMP3_HIDDEN_SIZE,
    CLAMP3_WEIGHTS_PATH,
    M3_HIDDEN_SIZE,
    MAX_AUDIO_LENGTH,
    MAX_TEXT_LENGTH,
    PATCH_LENGTH,
    PATCH_NUM_LAYERS,
    TEXT_MODEL_NAME,
)
from transformers import AutoTokenizer, BertConfig  # noqa: E402
from utils import CLaMP3Model  # noqa: E402

DEVICE = "mps" if torch.backends.mps.is_available() else "cpu"


class Clamp3:
    def __init__(self) -> None:
        audio_config = BertConfig(
            vocab_size=1,
            hidden_size=AUDIO_HIDDEN_SIZE,
            num_hidden_layers=AUDIO_NUM_LAYERS,
            num_attention_heads=AUDIO_HIDDEN_SIZE // 64,
            intermediate_size=AUDIO_HIDDEN_SIZE * 4,
            max_position_embeddings=MAX_AUDIO_LENGTH,
        )
        symbolic_config = BertConfig(
            vocab_size=1,
            hidden_size=M3_HIDDEN_SIZE,
            num_hidden_layers=PATCH_NUM_LAYERS,
            num_attention_heads=M3_HIDDEN_SIZE // 64,
            intermediate_size=M3_HIDDEN_SIZE * 4,
            max_position_embeddings=PATCH_LENGTH,
        )
        self.model = CLaMP3Model(
            audio_config=audio_config,
            symbolic_config=symbolic_config,
            text_model_name=TEXT_MODEL_NAME,
            hidden_size=CLAMP3_HIDDEN_SIZE,
            load_m3=False,
        )
        ckpt = torch.load(
            HERE / "weights" / CLAMP3_WEIGHTS_PATH,
            map_location="cpu",
            weights_only=True,
        )
        self.model.load_state_dict(ckpt["model"])
        self.model = self.model.to(DEVICE).eval()
        self.tokenizer = AutoTokenizer.from_pretrained(TEXT_MODEL_NAME)

    @torch.no_grad()
    def _global(
        self, data: torch.Tensor, max_len: int, pad: torch.Tensor, encode
    ) -> np.ndarray:
        # Split into model-sized segments, encode each to one vector, then
        # take the length-weighted mean (same as extract_clamp3.py).
        segments = [data[i : i + max_len] for i in range(0, len(data), max_len)]
        segments[-1] = data[-max_len:]
        vecs = []
        for seg in segments:
            mask = torch.cat([torch.ones(len(seg)), torch.zeros(max_len - len(seg))])
            seg = torch.cat([seg, pad.expand(max_len - len(seg), *pad.shape[1:])], 0)
            vecs.append(
                encode(seg.unsqueeze(0).to(DEVICE), mask.unsqueeze(0).to(DEVICE))
            )
        full, rest = divmod(len(data), max_len)
        weights = torch.tensor(
            [max_len] * full + ([rest] if rest else []), device=DEVICE
        ).view(-1, 1)
        out = (torch.cat(vecs, 0) * weights).sum(0) / weights.sum()
        return torch.nn.functional.normalize(out, dim=-1).cpu().numpy()

    def embed_text(self, text: str) -> np.ndarray:
        ids = self.tokenizer(text, return_tensors="pt")["input_ids"].squeeze(0)
        pad = torch.full((1,), self.tokenizer.pad_token_id, dtype=torch.long)
        return self._global(
            ids,
            MAX_TEXT_LENGTH,
            pad,
            lambda x, m: self.model.get_text_features(
                text_inputs=x, text_masks=m, get_global=True
            ),
        )

    def embed_mert(self, windows: np.ndarray) -> np.ndarray:
        """windows: MERT features, one 768-number row per 5-second window."""
        x = torch.tensor(windows, dtype=torch.float32).reshape(-1, windows.shape[-1])
        zero = torch.zeros((1, x.size(-1)))
        x = torch.cat([zero, x, zero], 0)
        return self._global(
            x,
            MAX_AUDIO_LENGTH,
            torch.zeros((1, AUDIO_HIDDEN_SIZE)),
            lambda a, m: self.model.get_audio_features(
                audio_inputs=a, audio_masks=m, get_global=True
            ),
        )


@lru_cache(maxsize=1)
def get_clamp3() -> Clamp3:
    return Clamp3()
