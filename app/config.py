"""CESG 业务后端配置（独立项目，不依赖任何外部工程）。

可通过环境变量 / .env 覆盖。所有 JT808 同步相关配置仅用于把
本系统的用户/公司基础档案下发到 808 平台（best-effort，失败不阻断本地）。
"""
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

    # 数据库（默认本项目 data/cesg.db；可用 DATABASE_URL 覆盖）
    database_url: str = f"sqlite+aiosqlite:///{_DEFAULT_DB.as_posix()}"

    # 服务监听端口（与旧 8000 区分，避免混淆）
    app_port: int = 8100

    # ---- JT808（灵星/808 平台）同步：把用户、公司基础档案下发到 808 ----
    jt808_sync_enabled: bool = True
    # 公司分组同步走 HTTP 8002 API（admin 登录）
    jt808_api_base: str = "http://113.207.68.96:8800/api"
    jt808_admin_account: str = "admin"
    jt808_admin_password: str = "123456"
    jt808_sync_timeout: float = 8.0
    # 用户同步走 SSH 隧道直连 MySQL jt808 库（127.0.0.1:3306）
    jt808_mysql_host: str = "127.0.0.1"
    jt808_mysql_port: int = 3306
    jt808_mysql_user: str = "root"
    jt808_mysql_password: str = "lgx123"
    jt808_mysql_database: str = "jt808"

    # ---- JT808 主动安全报警拉取（1208 等）----
    # 自建平台：http://113.207.68.96:8800/api + apicode 8003 登录（lingxtoken）
    # 公网 OpenAPI：https://www.gb35658.com/lingx3api + apicode 1200（apitoken）
    jt808_openapi_base_url: str = "http://113.207.68.96:8800/api"
    # 8003=自建 8800；1200=gb35658。留空则按 base_url / 是否配置 apitoken 自动判断。
    jt808_openapi_auth_mode: str = "8003"
    jt808_openapi_account: str = "admin"
    jt808_openapi_password: str = "123456"
    jt808_openapi_password_hashed: bool = False
    jt808_openapi_apitoken: str = ""
    jt808_openapi_timeout: float = 15.0

    # 默认关闭，配置好账号后再启用；避免开发环境启动后误拉生产数据。
    jt808_alarm_sync_enabled: bool = False
    jt808_alarm_sync_interval_seconds: int = 60
    jt808_alarm_sync_lookback_minutes: int = 5
    jt808_alarm_sync_page_size: int = 100
    jt808_alarm_sync_max_pages: int = 20

    # ---- OBD 时速违章监测：定时读 Redis OBD 数据，按私有地图规则判定超速 ----
    # 部署在服务器上时 Redis 走本机回环；本地开发可用 SSH 隧道改 host/port。
    obd_speed_check_enabled: bool = False
    obd_speed_check_interval_seconds: int = 30
    obd_redis_host: str = "127.0.0.1"
    obd_redis_port: int = 6379
    obd_redis_password: str = "lgx123"
    # JT808 平台 redis.properties 默认 database=1；与 808 共用实例时须读同一库
    obd_redis_db: int = 1
    obd_redis_key_pattern: str = "*_OBD"
    # 时速低于该值（km/h）不处理
    obd_min_speed_kmh: float = 10.0
    # OBD 读数 / 坐标快照超过该秒数视为过期，跳过判定
    obd_stale_seconds: int = 300
    # 限速折线的命中缓冲带（米）：车距折线多远内算"在该路段上"
    obd_polyline_buffer_m: float = 30.0

    # ---- Agent Worker AI（docs/AI.PDF）----
    agent_worker_base_url: str = "http://113.207.68.94:5002"
    agent_worker_api_key: str = ""
    agent_worker_default_company: str = "三峰城服"
    agent_worker_timeout: float = 60.0
    agent_worker_video_timeout: float = 300.0


settings = Settings()
