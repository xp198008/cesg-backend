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

    # ---- 新 JT808 OpenAPI：主动安全报警全局调度拉取 ----
    # 文档见 docs/api.docx；用于 1200/1210/1201/1208/1209。
    jt808_openapi_base_url: str = "https://www.gb35658.com/lingx3api"
    jt808_openapi_account: str = ""
    jt808_openapi_password: str = ""
    jt808_openapi_password_hashed: bool = False
    jt808_openapi_apitoken: str = ""
    jt808_openapi_timeout: float = 15.0

    # 默认关闭，配置好账号后再启用；避免开发环境启动后误拉生产数据。
    jt808_alarm_sync_enabled: bool = False
    jt808_alarm_sync_interval_seconds: int = 60
    jt808_alarm_sync_lookback_minutes: int = 5
    jt808_alarm_sync_page_size: int = 100
    jt808_alarm_sync_max_pages: int = 20


settings = Settings()
