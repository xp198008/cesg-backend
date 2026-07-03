"""知识图谱本地存储：按公司 + 16 分类目录，与 AI 知识库 category 字段对齐。

目录结构::
    data/knowledge_files/<公司名>/<两位分类号>/文件名

列表由扫描本地目录生成，不依赖 AI 接口。
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException

from app.config import settings

logger = logging.getLogger(__name__)

KNOWLEDGE_CATEGORIES: list[tuple[int, str]] = [
    (1, "一、安全目标"),
    (2, "二、安全机构和人员"),
    (3, "三、安全责任体系"),
    (4, "四、法规与制度"),
    (5, "五、安全投入"),
    (6, "六、装备设施"),
    (7, "七、科技创新与信息化"),
    (8, "八、队伍建设"),
    (9, "九、作业管理"),
    (10, "十、危险源辨识与风险控制"),
    (11, "十一、隐患排查与治理"),
    (12, "十二、职业健康"),
    (13, "十三、安全文化"),
    (14, "十四、应急救援"),
    (15, "十五、事故报告调查处理"),
    (16, "十六、安全绩效与持续改进"),
]

CATEGORY_NAME_BY_ID = {cid: name for cid, name in KNOWLEDGE_CATEGORIES}

KNOWLEDGE_ROOT = Path(__file__).resolve().parents[1] / "data" / "knowledge_files"

ALLOWED_EXTS = {
    ".doc", ".docx", ".pdf", ".txt", ".md",
    ".xls", ".xlsx", ".csv",
    ".ppt", ".pptx",
    ".png", ".jpg", ".jpeg", ".webp",
    ".zip", ".rar", ".7z",
}
MAX_UPLOAD_BYTES = 100 * 1024 * 1024


def company_dir_key(company: str) -> str:
    """公司目录名（与 AI x-company 一致，仅替换非法路径字符）。"""
    s = (company or settings.agent_worker_default_company or "default").strip()
    s = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", s)
    return s or "default"


def ensure_company_tree(company_key: str) -> Path:
    root = KNOWLEDGE_ROOT / company_key
    for cid, _ in KNOWLEDGE_CATEGORIES:
        (root / f"{cid:02d}").mkdir(parents=True, exist_ok=True)
    return root


def category_dir(company_key: str, category_id: int) -> Path:
    if category_id not in CATEGORY_NAME_BY_ID:
        raise HTTPException(status_code=404, detail="分类不存在")
    return ensure_company_tree(company_key) / f"{category_id:02d}"


def category_name(category_id: int) -> str:
    name = CATEGORY_NAME_BY_ID.get(category_id)
    if not name:
        raise HTTPException(status_code=404, detail="分类不存在")
    return name


def safe_filename(raw: str) -> str:
    name = Path(str(raw or "")).name.strip()
    if not name or name in {".", ".."}:
        raise HTTPException(status_code=400, detail="文件名无效")
    return name


def list_files_in_category(company_key: str, category_id: int) -> list[dict]:
    d = category_dir(company_key, category_id)
    rows: list[dict] = []
    for p in sorted(d.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
        if not p.is_file():
            continue
        st = p.stat()
        rows.append({
            "name": p.name,
            "size": st.st_size,
            "upload_time": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return rows


def build_catalog(company: str, company_key: str, dataset_id: str | None) -> dict:
    """按 16 类汇总本公司本地文档（扫描目录）。"""
    ensure_company_tree(company_key)
    categories = []
    total_files = 0
    for cid, cname in KNOWLEDGE_CATEGORIES:
        files = list_files_in_category(company_key, cid)
        total_files += len(files)
        categories.append({
            "id": cid,
            "name": cname,
            "file_count": len(files),
            "files": files,
        })
    return {
        "company": company,
        "company_key": company_key,
        "dataset_id": dataset_id,
        "total_files": total_files,
        "categories": categories,
    }


def migrate_legacy_flat_dirs() -> None:
    """旧版 knowledge_files/01..16 迁移到默认公司目录（仅执行一次）。"""
    default_key = company_dir_key(settings.agent_worker_default_company or "三峰城服")
    target_root = KNOWLEDGE_ROOT / default_key
    if target_root.exists() and any(target_root.iterdir()):
        return
    moved = 0
    for cid, _ in KNOWLEDGE_CATEGORIES:
        legacy = KNOWLEDGE_ROOT / f"{cid:02d}"
        if not legacy.is_dir():
            continue
        dest = ensure_company_tree(default_key) / f"{cid:02d}"
        for p in legacy.iterdir():
            if not p.is_file():
                continue
            target = dest / p.name
            if target.exists():
                stamp = datetime.now().strftime("%Y%m%d%H%M%S")
                target = dest / f"{p.stem}_{stamp}{p.suffix}"
            p.rename(target)
            moved += 1
        try:
            legacy.rmdir()
        except OSError:
            pass
    if moved:
        logger.info("知识图谱：已迁移 %s 个旧版文件到 %s/", moved, default_key)


KNOWLEDGE_ROOT.mkdir(parents=True, exist_ok=True)
migrate_legacy_flat_dirs()
