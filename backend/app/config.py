"""运行配置：端口、跨域、运行环境。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

_DEFAULT_EXPORT_DIR = str(Path(__file__).resolve().parents[1] / "exports")


@dataclass(frozen=True)
class Settings:
    app_name: str = "多源日志采集与解析中心"
    env: str = "local"
    port: int = 8013
    allowed_origins: list[str] = field(
        default_factory=lambda: [
            "http://127.0.0.1:5193",
            "http://localhost:5193",
        ]
    )
    page_size_default: int = 20
    page_size_max: int = 200
    # 交付文件（摘要/详单）与文件索引清单的落盘目录，可用环境变量覆盖。
    export_dir: str = field(
        default_factory=lambda: os.environ.get("LOG_INGEST_EXPORT_DIR", _DEFAULT_EXPORT_DIR)
    )


settings = Settings()
