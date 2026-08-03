"""CESG 涓氬姟鍚庣閰嶇疆锛堢嫭绔嬮」鐩紝涓嶄緷璧栦换浣曞閮ㄥ伐绋嬶級銆?
鍙€氳繃鐜鍙橀噺 / .env 瑕嗙洊銆傛墍鏈?JT808 鍚屾鐩稿叧閰嶇疆浠呯敤浜庢妸
鏈郴缁熺殑鐢ㄦ埛/鍏徃鍩虹妗ｆ涓嬪彂鍒?808 骞冲彴锛坆est-effort锛屽け璐ヤ笉闃绘柇鏈湴锛夈€?"""
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_BACKEND_DIR = Path(__file__).resolve().parent.parent
_DEFAULT_DB = _BACKEND_DIR / "data" / "cesg.db"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # 鏁版嵁搴擄紙榛樿鏈」鐩?data/cesg.db锛涘彲鐢?DATABASE_URL 瑕嗙洊锛?    database_url: str = f"sqlite+aiosqlite:///{_DEFAULT_DB.as_posix()}"

    # 鏈嶅姟鐩戝惉绔彛锛堜笌鏃?8000 鍖哄垎锛岄伩鍏嶆贩娣嗭級
    app_port: int = 8100

    # ---- JT808锛堢伒鏄?808 骞冲彴锛夊悓姝ワ細鎶婄敤鎴枫€佸叕鍙稿熀纭€妗ｆ涓嬪彂鍒?808 ----
    jt808_sync_enabled: bool = True
    # 鍏徃鍒嗙粍鍚屾璧?HTTP 8002 API锛坅dmin 鐧诲綍锛?    jt808_api_base: str = "http://113.207.68.96:8800/api"
    jt808_admin_account: str = "admin"
    jt808_admin_password: str = "123456"
    jt808_sync_timeout: float = 8.0
    # 鐢ㄦ埛鍚屾璧?SSH 闅ч亾鐩磋繛 MySQL jt808 搴擄紙127.0.0.1:3306锛?    jt808_mysql_host: str = "127.0.0.1"
    jt808_mysql_port: int = 3306
    jt808_mysql_user: str = "root"
    jt808_mysql_password: str = "lgx123"
    jt808_mysql_database: str = "jt808"

    # ---- JT808 涓诲姩瀹夊叏鎶ヨ鎷夊彇锛?208 绛夛級----
    # 鑷缓骞冲彴锛歨ttp://113.207.68.96:8800/api + apicode 8003 鐧诲綍锛坙ingxtoken锛?    # 鍏綉 OpenAPI锛歨ttps://www.gb35658.com/lingx3api + apicode 1200锛坅pitoken锛?    jt808_openapi_base_url: str = "http://113.207.68.96:8800/api"
    # 8003=鑷缓 8800锛?200=gb35658銆傜暀绌哄垯鎸?base_url / 鏄惁閰嶇疆 apitoken 鑷姩鍒ゆ柇銆?    jt808_openapi_auth_mode: str = "8003"
    jt808_openapi_account: str = "admin"
    jt808_openapi_password: str = "123456"
    jt808_openapi_password_hashed: bool = False
    jt808_openapi_apitoken: str = ""
    jt808_openapi_timeout: float = 15.0

    # 榛樿鍏抽棴锛岄厤缃ソ璐﹀彿鍚庡啀鍚敤锛涢伩鍏嶅紑鍙戠幆澧冨惎鍔ㄥ悗璇媺鐢熶骇鏁版嵁銆?    jt808_alarm_sync_enabled: bool = False
    jt808_alarm_sync_interval_seconds: int = 60
    jt808_alarm_sync_lookback_minutes: int = 5
    jt808_alarm_sync_page_size: int = 100
    jt808_alarm_sync_max_pages: int = 20

    # ---- OBD 鏃堕€熻繚绔犵洃娴嬶細瀹氭椂璇?Redis OBD 鏁版嵁锛屾寜绉佹湁鍦板浘瑙勫垯鍒ゅ畾瓒呴€?----
    # 閮ㄧ讲鍦ㄦ湇鍔″櫒涓婃椂 Redis 璧版湰鏈哄洖鐜紱鏈湴寮€鍙戝彲鐢?SSH 闅ч亾鏀?host/port銆?    obd_speed_check_interval_seconds: int = 30
    obd_redis_host: str = "127.0.0.1"
    obd_redis_port: int = 6379
    obd_redis_password: str = "lgx123"
    # JT808 骞冲彴 redis.properties 榛樿 database=1锛涗笌 808 鍏辩敤瀹炰緥鏃堕』璇诲悓涓€搴?    obd_redis_db: int = 1
    obd_redis_key_pattern: str = "*_OBD"
    # 鏃堕€熶綆浜庤鍊硷紙km/h锛変笉澶勭悊
    obd_min_speed_kmh: float = 10.0
    # OBD 璇绘暟 / 鍧愭爣蹇収瓒呰繃璇ョ鏁拌涓鸿繃鏈燂紝璺宠繃鍒ゅ畾
    obd_stale_seconds: int = 300
    # 闄愰€熸姌绾跨殑鍛戒腑缂撳啿甯︼紙绫筹級锛氳溅璺濇姌绾垮杩滃唴绠?鍦ㄨ璺涓?
    obd_polyline_buffer_m: float = 30.0

    # ---- 鏅烘収鐪嬫澘 Redis 闃熷垪娑堣垂锛圠POP锛?---
    # 澶嶇敤 obd_redis_* 杩炴帴鍙傛暟锛屼笉閲嶅閰嶇疆 host/port/password/db
    redis_queue_enabled: bool = True
    redis_queue_gzm: str = "QUEUE_GZM"
    redis_queue_obd_yc: str = "QUEUE_OBD_YC"
    redis_queue_obd_dc: str = "QUEUE_OBD_DC"
    # 姣忚疆 LPOP 鏉℃暟涓婇檺锛堥槻姝㈤槦鍒楀爢绉椂闀挎椂闂村崰鐢級
    redis_queue_batch_size: int = 200
    # 璋冨害闂撮殧锛堢锛?    redis_queue_interval_seconds: int = 5
    # 鏁呴殰璁板綍淇濈暀鏃堕暱锛堝皬鏃讹級锛岃秴鏃惰嚜鍔ㄦ竻鐞?    redis_queue_fault_ttl_hours: int = 72

    # ---- Agent Worker AI锛坉ocs/AI.PDF锛?---
    agent_worker_base_url: str = "http://113.207.68.94:5002"
    agent_worker_api_key: str = ""
    agent_worker_default_company: str = "涓夊嘲鍩庢湇"
    agent_worker_timeout: float = 60.0
    agent_worker_video_timeout: float = 300.0


    # ---- 车辆风险画像（docs/2.pdf，周报；月报官方不可用时由周报拼接）----
    risk_api_base_url: str = "http://113.207.68.94:8000"
    risk_api_timeout: float = 20.0


settings = Settings()
