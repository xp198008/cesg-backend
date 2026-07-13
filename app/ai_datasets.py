"""Agent Worker 知识库 dataset_id 对照（docs/AI.PDF 第四节）。"""



from __future__ import annotations



from app.config import settings



# 知识库名称 -> dataset_id（UUID）

AI_DATASETS: dict[str, str] = {

    "垫江公司": "662ad5c8-85a4-48f7-ab8e-8ea0de39a879",

    "合川分公司": "6c83f810-415c-494f-affd-5af95cbd3be4",

    "江津公司": "40e10b47-4cfc-471e-aafb-2e276d28c239",

    "铜梁公司": "6075ba3b-e494-4570-b47d-09be59ce970a",

    "渝环公司": "d6308795-2649-43aa-aa17-1ac1f4e33071",

    "长寿公司": "638546ca-50a6-43e8-a887-9c7f74b82820",

    "三峰城服": "7c2784c2-3267-4a0a-b231-22e1d9d9c420",

    "固废运输公司": "d4008312-1c7f-43c2-869a-1ff2996a2b72",

    "涪陵公司": "95147708-4e16-4f5a-9a14-c9430f3d259e",

    "綦江公司": "90dc6d46-604f-4601-ab06-a358dae461a2",

    "益康工程": "16cbe50f-c04b-4c5c-81da-6e158ac7c0df",

    "南岸公司": "563e5ef6-514d-4619-b84d-50811c521adc",

    "黔江公司": "40cdaf34-db61-45ee-9a65-3d8bf7b2be86",

    "环卫集团及水务环境集团": "818405f2-283e-4a40-b468-9b8b80091bac",

    "永川公司": "65a53e19-9e67-4824-9d0d-8fec98bf5867",

    "璧山公司": "b9e1bf7c-637e-422f-94ad-be00d7d76af2",

    "固废处理公司": "2bd3ae66-0b19-410b-a56c-9b0709f83ffc",

    "南川公司": "12371ada-11c8-4431-ace5-b17a23e7ea56",

    "水域公司": "a01e9b6a-ae0c-4ff9-bc76-42ce6f177cb4",

    "益渝公司": "bee8ab3a-f539-474f-affc-ce205af887c5",

    "北碚公司": "5aa74f41-1b7d-4e7d-867a-6516c341a2fc",

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

