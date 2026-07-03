"""Agent Worker 知识库 dataset_id 对照（docs/AI.PDF 第四节）。"""

from __future__ import annotations

from app.config import settings

# 知识库名称 -> dataset_id（UUID）
AI_DATASETS: dict[str, str] = {
    "垫江公司": "7845fcd3-2332-45ac-a7ab-dc3fbe163455",
    "合川分公司": "5b24ac06-a9fd-4f85-ad4d-1488b97765d4",
    "江津公司": "07f20588-cae0-45fd-8dfe-b1a7e669aebd",
    "铜梁公司": "7c67473b-3793-4bbe-82ff-ee47c3a70b33",
    "渝环公司": "7944568e-548a-4e21-a02b-d2b76fd4f642",
    "长寿公司": "e329a9d4-de57-49a9-905e-aae8451364ae",
    "三峰城服": "aea0a41f-2257-431d-95bb-91cf3f3cb5cc",
    "固废运输公司": "6df0852f-0eb7-4ba4-a082-883d9729b896",
    "涪陵公司": "98697f6f-1a43-4152-9d4b-c04056acaf03",
    "綦江公司": "f914c86f-e65e-46b8-9f17-e47b2c1753ee",
    "益康工程": "2bd1751e-d7ec-4e49-ac17-c0ac21d15e4d",
    "南岸公司": "a6d485a0-4857-463a-9df8-9c9e947366a0",
    "黔江公司": "7dff0cba-b600-4ec5-855f-d9cabc4e80a0",
    "环卫集团及水务环境集团": "31000bd4-0bd4-4af5-ad77-19bfd12b2f4d",
    "永川公司": "2e96934f-a342-426d-9a13-3008c6688dcd",
    "璧山公司": "c3ac9e17-fe94-419b-9f4a-1b66248c1894",
    "固废处理公司": "92999a64-a12e-471a-8a51-1bc7190287d5",
    "南川公司": "dfd36c1c-a428-412c-9e68-5161ef688272",
    "水域公司": "483810ad-18cf-4b8f-b9eb-aa951ee3e5fb",
    "益渝公司": "4c12b7ba-718b-4add-ba90-f02738d8530b",
    "北碚公司": "0a0199c1-67b3-4ab1-aa82-af0a85ac0f11",
}


def match_ai_company(org_name: str | None) -> str | None:
    """机构名 → AI 知识库公司名；匹配不到返回 None（不兜底）。"""
    name = (org_name or "").strip()
    if not name:
        return None

    if name in AI_DATASETS:
        return name

    for key in AI_DATASETS:
        if key in name or name in key:
            return key

    simplified = (
        name.replace("重庆市", "")
        .replace("重庆", "")
        .replace("有限责任公司", "")
        .replace("有限公司", "")
        .replace("分公司", "")
        .strip()
    )
    if not simplified:
        return None
    for key in AI_DATASETS:
        ks = key.replace("分公司", "").replace("公司", "").strip()
        if ks and (ks in simplified or simplified.startswith(ks) or ks.startswith(simplified)):
            return key

    return None


def resolve_ai_company(org_name: str | None, *, override: str | None = None) -> str:
    """将 CESG 机构名映射为 Agent Worker 的 x-company（知识库/规章查询用）。"""
    if override and override.strip():
        return override.strip()

    matched = match_ai_company(org_name)
    if matched:
        return matched
    return (settings.agent_worker_default_company or "三峰城服").strip()


def resolve_dataset_id(company: str) -> str | None:
    return AI_DATASETS.get((company or "").strip())
