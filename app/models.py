"""CESG 业务数据模型（用户/角色/组织/车辆/司机/设备 + 审计日志）。

表名与列名与既有数据库一致，便于直接复用历史数据。
"""
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Float,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from app.database import Base
from app.timeutil import china_now_naive


class SysRole(Base):
    __tablename__ = "sys_role"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False)
    code = Column(String(64), unique=True)
    remark = Column(String(512))
    org_id = Column(Integer, ForeignKey("org_company.id"), nullable=True)
    is_global = Column(Boolean, nullable=False, default=False, server_default="0")
    permissions = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)

    org = relationship("OrgCompany", backref="roles")


class SysUser(Base):
    __tablename__ = "sys_user"
    __table_args__ = (
        UniqueConstraint("username", "password_hash", name="uq_sys_user_username_password_hash"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    password_hash = Column(String(256), nullable=False)
    password_plain = Column(String(128), nullable=True)
    real_name = Column(String(64))
    identity = Column(String(64), nullable=True)
    phone = Column(String(32), nullable=True)
    role_id = Column(Integer, ForeignKey("sys_role.id"))
    org_id = Column(Integer, ForeignKey("org_company.id"))
    jt808_user_id = Column(String(36), nullable=True, index=True)
    allow_pwd_edit = Column(Boolean, default=True, nullable=False)
    is_active = Column(Boolean, default=True)
    valid_until = Column(Date, nullable=True)
    single_login = Column(Boolean, default=False, nullable=False, server_default="0")
    login_session_token = Column(String(64), nullable=True)
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)
    remark = Column(String(256))

    role = relationship("SysRole", backref="users")
    org = relationship("OrgCompany", backref="users")
    vehicle_alloc_rules = relationship("VehicleAllocRule", secondary="vehicle_alloc_rule_user")
    shortcuts = relationship("SysUserShortcut", back_populates="user", cascade="all, delete-orphan")


class SysUserShortcut(Base):
    __tablename__ = "sys_user_shortcut"
    __table_args__ = (
        UniqueConstraint("user_id", "permission_id", name="uq_sys_user_shortcut_user_permission"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("sys_user.id", ondelete="CASCADE"), nullable=False, index=True)
    permission_id = Column(String(32), nullable=False, index=True)
    title = Column(String(128), nullable=False)
    url = Column(String(256), nullable=False)
    icon = Column(String(256), nullable=True)
    sort_order = Column(Integer, nullable=False, default=0, server_default="0")
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)

    user = relationship("SysUser", back_populates="shortcuts")


class OrgCompany(Base):
    __tablename__ = "org_company"
    id = Column(Integer, primary_key=True, autoincrement=True)
    org_code = Column(String(48), unique=True, index=True, nullable=True)
    name = Column(String(128), nullable=False)
    short_name = Column(String(64))
    legal_person = Column(String(64))
    parent_id = Column(
        Integer, ForeignKey("org_company.id", ondelete="RESTRICT"), nullable=True, index=True
    )
    contact = Column(String(64))
    contact_phone = Column(String(32))
    address = Column(String(256))
    remark = Column(String(256))
    jt808_group_id = Column(Integer, nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)

    parent = relationship(
        "OrgCompany", remote_side=[id], foreign_keys=[parent_id], backref="children"
    )
    fleets = relationship("Fleet", back_populates="company")


class Fleet(Base):
    __tablename__ = "fleet"
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("org_company.id"), nullable=False)
    name = Column(String(128), nullable=False)
    remark = Column(String(256))
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)

    company = relationship("OrgCompany", back_populates="fleets")


class Driver(Base):
    __tablename__ = "driver"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False)
    company_id = Column(
        Integer, ForeignKey("org_company.id", ondelete="RESTRICT"), nullable=True, index=True
    )
    gender = Column(String(8), nullable=True)
    certificate_code = Column(String(64), nullable=True)
    id_card = Column(String(32), nullable=True)
    phone = Column(String(32), nullable=True)
    birth_date = Column(Date, nullable=True)
    entry_date = Column(Date, nullable=True)
    license_issue_date = Column(Date, nullable=True)
    driver_license_no = Column(String(64), nullable=True)
    driver_type = Column(String(16), nullable=True)
    license_expiry = Column(String(32), nullable=True)
    drive_hours = Column(Integer, nullable=True)
    drive_mileage = Column(Integer, nullable=True)
    score = Column(Integer, nullable=True)
    native_place = Column(String(128), nullable=True)
    avatar_url = Column(String(256), nullable=True)
    remark = Column(String(256))
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)

    company = relationship("OrgCompany", backref="drivers")
    vehicles = relationship("Vehicle", back_populates="driver")


class Vehicle(Base):
    __tablename__ = "vehicle"
    id = Column(Integer, primary_key=True, autoincrement=True)
    plate_no = Column(String(16), nullable=False, unique=True, index=True)
    plate_color = Column(String(16), default="黄牌")
    vehicle_category = Column(String(16), nullable=True)
    vehicle_type = Column(String(32))
    vehicle_type_ii = Column(String(32))
    color = Column(String(32))
    vin = Column(String(64))
    driving_license_no = Column(String(64))
    engine_no = Column(String(64))
    short_name = Column(String(64))
    company_id = Column(Integer, ForeignKey("org_company.id"))
    company_org_code = Column(String(48), index=True, nullable=True)
    fleet_id = Column(Integer, ForeignKey("fleet.id"))
    driver_id = Column(
        Integer, ForeignKey("driver.id", ondelete="SET NULL"), nullable=True, index=True
    )
    driver_name = Column(String(64), nullable=True)
    owner_name = Column(String(64))
    contact_name = Column(String(64))
    contact_phone = Column(String(32))
    legal_contact_phone = Column(String(32))
    legal_address = Column(String(256))
    route = Column(String(128))
    agent = Column(String(128))
    install_date = Column(Date)
    service_start_date = Column(Date)
    service_end_date = Column(Date)
    status = Column(String(16), default="正常")
    last_online_at = Column(DateTime(timezone=True))
    channel_count = Column(Integer, default=0)
    engine_displacement = Column(String(32), nullable=True)
    fuel_tank_capacity = Column(String(32), nullable=True)
    battery_capacity = Column(String(32), nullable=True)
    range_mileage = Column(String(32), nullable=True)
    battery_no = Column(String(64), nullable=True)
    motor_no = Column(String(64), nullable=True)
    manufacturer = Column(String(64))
    brand = Column(String(64))
    model = Column(String(64))
    vehicle_grade = Column(String(32))
    vehicle_usage = Column(String(64))
    speed_limit = Column(Numeric(10, 2), default=0)
    track_retain_days = Column(Integer, default=0)
    mileage_factor = Column(Numeric(6, 2))
    mileage_offset = Column(Numeric(10, 2), nullable=True)
    scrap_date = Column(Date)
    inspect_date = Column(Date)
    plate_login = Column(Boolean, default=False)
    is_connect = Column(Boolean, default=False)
    acc_on = Column(Boolean, default=False, nullable=False)
    gps_satellite_count = Column(Integer, nullable=True)
    night_speed_enabled = Column(Boolean, default=False)
    night_start_time = Column(String(16))
    night_end_time = Column(String(16))
    night_speed_percent = Column(Numeric(5, 2))
    icon_id = Column(Integer, default=1)
    remark = Column(Text)
    created_by = Column(Integer)
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)

    company = relationship("OrgCompany", backref="vehicles")
    fleet = relationship("Fleet", backref="vehicles")
    driver = relationship("Driver", back_populates="vehicles")
    devices = relationship(
        "VehicleDevice", back_populates="vehicle", cascade="all, delete-orphan"
    )
    alloc_rules = relationship("VehicleAllocRule", secondary="vehicle_alloc_rule_vehicle")


class VehicleDevice(Base):
    __tablename__ = "vehicle_device"
    id = Column(Integer, primary_key=True, autoincrement=True)
    vehicle_id = Column(Integer, ForeignKey("vehicle.id"), nullable=False)
    device_no = Column(String(64), nullable=False, unique=True, index=True)
    device_sn = Column(String(64))
    terminal_type = Column(String(32))
    sim_no = Column(String(32))
    actual_sim = Column(String(32))
    product_model = Column(String(64))
    channels = Column(JSON, nullable=True)
    is_main = Column(Boolean, default=True)
    channel_no = Column(Integer, default=1)
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)

    vehicle = relationship("Vehicle", back_populates="devices")


class VehicleTypeDict(Base):
    """基础数据：车辆类型维护。"""

    __tablename__ = "vehicle_type_dict"
    id = Column(Integer, primary_key=True, autoincrement=True)
    type_code = Column(String(32), nullable=False, index=True)
    type_name = Column(String(64), nullable=False, index=True)
    icon_url = Column(String(256), nullable=True)
    spec = Column(String(256), nullable=True)
    site = Column(String(128), nullable=True)
    sort_order = Column(Integer, nullable=False, default=0, server_default="0")
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)


class ReserveTerminal(Base):
    __tablename__ = "reserve_terminal"
    id = Column(Integer, primary_key=True, autoincrement=True)
    terminal_id = Column(String(64), unique=True, index=True)
    first_auth_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    last_auth_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    last_peer = Column(String(256))
    remark = Column(Text)


class VehicleAllocRule(Base):
    """车辆分配规则：公司/车队维度维护管控车辆和分配用户。"""

    __tablename__ = "vehicle_alloc_rule"
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("org_company.id", ondelete="CASCADE"), nullable=False, index=True)
    fleet_id = Column(Integer, ForeignKey("fleet.id", ondelete="CASCADE"), nullable=True, index=True)
    name = Column(String(128), nullable=False)
    remark = Column(String(512))
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)

    company = relationship("OrgCompany", backref="vehicle_alloc_rules")
    fleet = relationship("Fleet", backref="vehicle_alloc_rules")
    vehicles = relationship("Vehicle", secondary="vehicle_alloc_rule_vehicle")
    users = relationship("SysUser", secondary="vehicle_alloc_rule_user")


class VehicleAllocRuleVehicle(Base):
    """车辆分配规则与管控车辆关联。"""

    __tablename__ = "vehicle_alloc_rule_vehicle"
    rule_id = Column(Integer, ForeignKey("vehicle_alloc_rule.id", ondelete="CASCADE"), primary_key=True)
    vehicle_id = Column(Integer, ForeignKey("vehicle.id", ondelete="CASCADE"), primary_key=True)


class VehicleAllocRuleUser(Base):
    """车辆分配规则与分配用户关联。"""

    __tablename__ = "vehicle_alloc_rule_user"
    rule_id = Column(Integer, ForeignKey("vehicle_alloc_rule.id", ondelete="CASCADE"), primary_key=True)
    user_id = Column(Integer, ForeignKey("sys_user.id", ondelete="CASCADE"), primary_key=True)


class AlarmTypeDict(Base):
    """基础数据：报警类型字典，供本地安全/报警业务选用。"""

    __tablename__ = "alarm_type_dict"
    id = Column(Integer, primary_key=True, autoincrement=True)
    type_code = Column(String(32), nullable=False, unique=True, index=True)
    type_name = Column(String(64), nullable=False, index=True)
    description = Column(Text, nullable=True)
    alarm_level = Column(String(16), nullable=False, server_default="中级")
    data_source = Column(String(32), nullable=False, server_default="manual")
    ttx_atp_code = Column(Integer, nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)


class ViolationTypeDict(Base):
    """违章/主动安全类型字典，供主动安全报警处理页面展示严重程度。"""

    __tablename__ = "violation_type_dict"
    id = Column(Integer, primary_key=True, autoincrement=True)
    type_code = Column(String(32), nullable=False, unique=True, index=True)
    type_name = Column(String(64), nullable=False, index=True)
    description = Column(Text, nullable=True)
    severity = Column(String(16), nullable=False, server_default="一般")
    deduction_score = Column(Integer, nullable=False, server_default="0")
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)


class FaultTypeDict(Base):
    """基础数据：故障类型字典，供人工报障等业务选用。"""

    __tablename__ = "fault_type_dict"
    id = Column(Integer, primary_key=True, autoincrement=True)
    type_code = Column(String(32), nullable=False, unique=True, index=True)
    type_name = Column(String(64), nullable=False, index=True)
    description = Column(Text, nullable=True)
    fault_level = Column(String(16), nullable=False, server_default="中")
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)


class VehicleLocation(Base):
    """车辆实时位置快照，供主动安全处理弹窗地图定位等场景使用。"""

    __tablename__ = "vehicle_location"
    id = Column(Integer, primary_key=True, autoincrement=True)
    vehicle_id = Column(Integer, ForeignKey("vehicle.id"), nullable=False, unique=True, index=True)
    plate_no = Column(String(32), nullable=False, index=True)
    company_id = Column(Integer, index=True)
    terminal_id = Column(String(64), index=True)
    lat = Column(Float)
    lng = Column(Float)
    speed = Column(Float)
    pos_time = Column(DateTime)
    current_position = Column(String(512))
    is_online = Column(Boolean, default=False)
    source = Column(String(32), default="ttx")
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive, default=china_now_naive, server_default=func.now())


class MapApiConfig(Base):
    """地图 API 配置，供地图接口管理和公用限速地图初始化使用。"""

    __tablename__ = "map_api_config"
    id = Column(Integer, primary_key=True, autoincrement=True)
    provider = Column(String(50), nullable=False, unique=True, default="amap")
    api_key = Column(String(255))
    secret_key = Column(String(255))
    default_zoom = Column(Integer, default=12)
    default_center_lng = Column(Float, default=106.55156)
    default_center_lat = Column(Float, default=29.56301)
    remark = Column(String(255))
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)


class PublicMapRule(Base):
    """公用地图规则：公用限速管理页绘制和维护的围栏/折线规则。"""

    __tablename__ = "public_map_rule"
    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_code = Column(String(64), nullable=False, unique=True, index=True)
    rule_name = Column(String(200), nullable=False)
    rule_type_code = Column(String(32), nullable=False)
    draw_shape_type = Column(String(32), nullable=False)
    is_public = Column(Integer, nullable=False, default=1, server_default="1")
    geometry_json = Column(JSON, nullable=False)
    remark = Column(String(255))
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)


class PrivateMapRule(Base):
    """私有地图规则：地图管理页按公司维护的围栏/限速规则。"""

    __tablename__ = "private_map_rule"
    id = Column(Integer, primary_key=True, autoincrement=True)
    company_id = Column(Integer, ForeignKey("org_company.id"), nullable=False, index=True)
    rule_code = Column(String(64), nullable=False, unique=True, index=True)
    rule_name = Column(String(200), nullable=False)
    rule_type_code = Column(String(32), nullable=False)
    draw_shape_type = Column(String(32), nullable=False)
    geometry_json = Column(JSON, nullable=False)
    speed_limit_kmh = Column(Integer, nullable=False, default=0, server_default="0")
    ref_public_rule_id = Column(Integer, nullable=True, index=True)
    category_ids = Column(JSON, nullable=False, default=list)
    remark = Column(String(255))
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)

    company = relationship("OrgCompany", backref="private_map_rules")


class PrivateMapRuleWeather(Base):
    """私有地图规则下的天气限速明细。"""

    __tablename__ = "private_map_rule_weather"
    id = Column(Integer, primary_key=True, autoincrement=True)
    private_map_rule_id = Column(
        Integer, ForeignKey("private_map_rule.id", ondelete="CASCADE"), nullable=False, index=True
    )
    weather_type_code = Column(String(32), nullable=False)
    speed_limit_kmh = Column(Integer, nullable=False)
    sort_order = Column(Integer, nullable=False, default=0, server_default="0")
    remark = Column(String(255))
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)

    rule = relationship("PrivateMapRule", backref="weather_rules")


class MapRuleCategory(Base):
    """地图规则类别：按公司维护类别限速、天气规则和分配车辆。"""

    __tablename__ = "map_rule_category"
    id = Column(Integer, primary_key=True, autoincrement=True)
    type_name = Column(String(128), nullable=False)
    company_id = Column(Integer, ForeignKey("org_company.id"), nullable=False, index=True)
    speed_limit_kmh = Column(Integer, nullable=False, default=0, server_default="0")
    weather_rule_id = Column(Integer, ForeignKey("private_map_rule_weather.id", ondelete="SET NULL"), nullable=True, index=True)
    weather_types = Column(JSON, nullable=False, default=list)
    weather_speed_limits = Column(JSON, nullable=False, default=dict)
    assigned_vehicle_ids = Column(JSON, nullable=False, default=list)
    remark = Column(String(255))
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=china_now_naive)

    company = relationship("OrgCompany", backref="map_rule_categories")
    weather_rule = relationship("PrivateMapRuleWeather")


class VehicleViolation(Base):
    """主动安全/违章报警记录，兼容旧项目安全管理页面。"""

    __tablename__ = "vehicle_violation"
    id = Column(Integer, primary_key=True, autoincrement=True)
    biz_no = Column(String(32), nullable=False, unique=True, index=True)
    external_alarm_id = Column(String(128), nullable=True, unique=True, index=True)
    terminal_id = Column(String(32), nullable=False, index=True)
    vehicle_id = Column(Integer, index=True, nullable=True)
    plate_no = Column(String(16), default="", index=True)
    company_id = Column(Integer, nullable=True, index=True)
    violation_type_code = Column(Integer, nullable=True)
    violation_type_name = Column(String(64), nullable=True)
    violation_time = Column(DateTime, nullable=False, index=True)
    lat = Column(Float, nullable=True)
    lng = Column(Float, nullable=True)
    address = Column(String(512), nullable=True)
    source = Column(String(32), nullable=False)
    transparent_type = Column(Integer, nullable=True)
    raw_preview = Column(Text, nullable=True)
    stream_snapshot_refs = Column(Text, nullable=True)
    ttx_evidence_refs = Column(Text, nullable=True)
    status = Column(String(16), nullable=False, default="待处理")
    pre_audit_kind = Column(String(16), nullable=True)
    ticket_appeal_remark = Column(Text, nullable=True)
    ticket_appeal_attachment_refs = Column(Text, nullable=True)
    handler_remark = Column(Text, nullable=True)
    handler_name = Column(String(64), nullable=True)
    handled_at = Column(DateTime, nullable=True)
    auditor_name = Column(String(64), nullable=True)
    audited_at = Column(DateTime, nullable=True)
    audit_reject_remark = Column(Text, nullable=True)
    appeal_reason = Column(Text, nullable=True)
    appeal_submitted_at = Column(DateTime, nullable=True)
    appeal_status = Column(String(16), nullable=True)
    ai_queried = Column(Boolean, default=False, nullable=False, server_default="0")
    created_at = Column(DateTime, default=china_now_naive, server_default=func.now())


class ViolationAiAssessment(Base):
    """主动安全报警 AI 首次评估与罚单建议（与 vehicle_violation 一对一）。"""

    __tablename__ = "violation_ai_assessment"
    __table_args__ = (UniqueConstraint("violation_id", name="uq_violation_ai_assessment_violation_id"),)
    id = Column(Integer, primary_key=True, autoincrement=True)
    violation_id = Column(Integer, ForeignKey("vehicle_violation.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    session_id = Column(String(128), nullable=True)
    evaluation_text = Column(Text, nullable=True)
    ticket_process_type = Column(String(64), nullable=True)
    ticket_amount = Column(Float, nullable=True, default=0.0)
    ticket_basis = Column(Text, nullable=True)
    ticket_suggestion_text = Column(Text, nullable=True)
    evidence_valid = Column(Boolean, nullable=True)
    system_judgment_correct = Column(Boolean, nullable=True)
    violated_rules_json = Column(Text, nullable=True)
    video_analysis_text = Column(Text, nullable=True)
    raw_response_text = Column(Text, nullable=True)
    company_name = Column(String(128), nullable=True)
    alarm_type_name = Column(String(64), nullable=True)
    image_count = Column(Integer, nullable=False, default=0, server_default="0")
    has_video = Column(Boolean, nullable=False, default=False, server_default="0")
    created_at = Column(DateTime, default=china_now_naive, server_default=func.now())
    updated_at = Column(DateTime, onupdate=china_now_naive)

    violation = relationship("VehicleViolation", backref="ai_assessment", uselist=False)


class ViolationTicket(Base):
    """主动安全报警关联罚单信息。"""

    __tablename__ = "violation_ticket"
    __table_args__ = (UniqueConstraint("biz_no", name="uq_violation_ticket_biz_no"),)
    id = Column(Integer, primary_key=True, autoincrement=True)
    biz_no = Column(String(32), nullable=False, index=True)
    violation_id = Column(Integer, nullable=True, index=True)
    process_type = Column(String(64), nullable=False)
    remark = Column(Text, nullable=True)
    amount = Column(Float, nullable=False, default=0.0)
    status = Column(String(16), nullable=False, default="待处理")
    created_by_name = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=china_now_naive, server_default=func.now())


class Jt808AlarmSyncState(Base):
    """新 JT808 OpenAPI 主动安全同步游标。"""

    __tablename__ = "jt808_alarm_sync_state"
    source = Column(String(32), primary_key=True)
    last_window_start_at = Column(DateTime, nullable=True)
    last_window_end_at = Column(DateTime, nullable=True)
    last_success_at = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=True)
    last_total = Column(Integer, nullable=False, default=0, server_default="0")
    last_inserted = Column(Integer, nullable=False, default=0, server_default="0")
    updated_at = Column(DateTime, default=china_now_naive, server_default=func.now(), onupdate=china_now_naive)


class UserLoginLog(Base):
    __tablename__ = "user_login_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=True, index=True)
    username = Column(String(64), nullable=False, index=True)
    real_name = Column(String(64), nullable=True)
    org_id = Column(Integer, nullable=True)
    org_name = Column(String(128), nullable=True)
    role_id = Column(Integer, nullable=True)
    role_name = Column(String(64), nullable=True)
    login_ip = Column(String(64), nullable=False, server_default="")
    login_method = Column(String(32), nullable=False, server_default="web")
    login_at = Column(DateTime, nullable=False, default=china_now_naive, server_default=func.now(), index=True)
    logout_at = Column(DateTime, nullable=True, index=True)
    online_seconds = Column(Integer, nullable=True)
    last_heartbeat_at = Column(DateTime, nullable=True)


class UserOnlineDaily(Base):
    """用户按日在线时长汇总（报表 user-online-duration 数据源）。"""
    __tablename__ = "user_online_daily"
    __table_args__ = (UniqueConstraint("username", "stat_date", name="uq_user_online_daily_username_date"),)
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=True, index=True)
    username = Column(String(64), nullable=False, index=True)
    real_name = Column(String(64), nullable=True)
    org_id = Column(Integer, nullable=True)
    org_name = Column(String(128), nullable=True)
    stat_date = Column(Date, nullable=False, index=True)
    online_seconds = Column(Integer, nullable=False, default=0, server_default="0")
    login_count = Column(Integer, nullable=False, default=0, server_default="0")
    updated_at = Column(DateTime, default=china_now_naive, server_default=func.now(), onupdate=china_now_naive)


class ManualFaultReport(Base):
    """人工报障录入（与 jt_device_fault 分流）。"""

    __tablename__ = "manual_fault_report"
    id = Column(Integer, primary_key=True, autoincrement=True)
    biz_no = Column(String(32), nullable=False, unique=True, index=True)
    plate_no = Column(String(16), nullable=False, index=True)
    terminal_bind_no = Column(String(64), nullable=True, index=True)
    vehicle_id = Column(Integer, nullable=True, index=True)
    company_id = Column(Integer, nullable=True, index=True)
    fault_type_dict_id = Column(Integer, nullable=False, index=True)
    fault_type_name = Column(String(64), nullable=True)
    fault_level = Column(String(16), nullable=False)
    discovery_time = Column(DateTime, nullable=False, index=True)
    discoverer = Column(String(64), nullable=False)
    fault_devices = Column(Text, nullable=True)
    fault_phenomenon = Column(Text, nullable=True)
    fault_location = Column(String(256), nullable=True)
    affect_service = Column(Integer, nullable=False, server_default="1")
    handle_status = Column(String(32), nullable=False, server_default="未处理")
    handled_at = Column(DateTime, nullable=True)
    handler_name = Column(String(64), nullable=True)
    handler_remark = Column(String(255), nullable=True)
    audited_at = Column(DateTime, nullable=True)
    auditor_name = Column(String(64), nullable=True)
    audit_remark = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), default=china_now_naive, server_default=func.now())


class JtDeviceFault(Base):
    """终端/车载故障（0x0200 报警位），供报障处理页。"""

    __tablename__ = "jt_device_fault"
    id = Column(Integer, primary_key=True, autoincrement=True)
    biz_no = Column(String(32), nullable=False, unique=True, index=True)
    terminal_id = Column(String(32), nullable=False, index=True)
    vehicle_id = Column(Integer, index=True, nullable=True)
    plate_no = Column(String(16), default="", index=True)
    company_id = Column(Integer, nullable=True, index=True)
    fault_bit = Column(Integer, nullable=False, index=True)
    fault_type_name = Column(String(64), nullable=True)
    fault_time = Column(DateTime, nullable=False, index=True)
    alarm_flags = Column(Integer, nullable=False)
    lat = Column(Float, nullable=True)
    lng = Column(Float, nullable=True)
    speed_kmh = Column(Float, nullable=True)
    direction = Column(Integer, nullable=True)
    raw_preview = Column(Text, nullable=True)
    source = Column(String(32), nullable=False, default="jt808_0200_alarm")
    handle_status = Column(String(32), default="未处理")
    handled_at = Column(DateTime, nullable=True)
    handler_name = Column(String(64), nullable=True)
    handler_remark = Column(String(255), nullable=True)
    audited_at = Column(DateTime, nullable=True)
    auditor_name = Column(String(64), nullable=True)
    audit_remark = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=china_now_naive, server_default=func.now())


class JtDeviceFaultReceipt(Base):
    """设备报障结案后的上传单据索引。"""

    __tablename__ = "jt_device_fault_receipt"
    id = Column(Integer, primary_key=True, autoincrement=True)
    fault_id = Column(Integer, nullable=False, index=True)
    biz_no = Column(String(32), nullable=False, index=True)
    stored_name = Column(String(255), nullable=False)
    original_name = Column(String(255), nullable=False)
    file_size = Column(Integer, nullable=False)
    mime_type = Column(String(128), nullable=True)
    uploader_name = Column(String(64), nullable=True)
    created_at = Column(DateTime, default=china_now_naive, server_default=func.now())


class VehicleRepair(Base):
    """设备报修工单（人工录入 + 审核 + 维修状态跟踪）。"""

    __tablename__ = "vehicle_repair"
    id = Column(Integer, primary_key=True, autoincrement=True)
    biz_no = Column(String(32), nullable=False, unique=True, index=True)
    plate_no = Column(String(16), nullable=False, index=True)
    vehicle_id = Column(Integer, nullable=True, index=True)
    company_id = Column(Integer, nullable=True, index=True)
    repair_type = Column(String(32), nullable=False, server_default="设备报修")
    repair_time = Column(DateTime, nullable=False, index=True)
    repairer = Column(String(64), nullable=False)
    phone = Column(String(32), nullable=True)
    expected_at = Column(DateTime, nullable=True)
    main_device = Column(String(64), nullable=True)
    device_model = Column(String(64), nullable=True)
    device_no = Column(String(64), nullable=True)
    description = Column(Text, nullable=True)
    repair_address = Column(String(256), nullable=True)
    estimated_cost = Column(Numeric(10, 2), nullable=True)
    remark = Column(Text, nullable=True)
    review_status = Column(String(32), nullable=False, server_default="待审核", index=True)
    reviewer = Column(String(64), nullable=True)
    review_remark = Column(String(255), nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    repair_status = Column(String(32), nullable=False, server_default="待处理", index=True)
    completed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=china_now_naive, server_default=func.now())


class VehicleRepairReceipt(Base):
    """设备报修单据（维修工单/发票/配件清单等附件索引）。"""

    __tablename__ = "vehicle_repair_receipt"
    id = Column(Integer, primary_key=True, autoincrement=True)
    repair_id = Column(Integer, nullable=False, index=True)
    biz_no = Column(String(32), nullable=False, index=True)
    stored_name = Column(String(255), nullable=False)
    original_name = Column(String(255), nullable=False)
    file_size = Column(Integer, nullable=False)
    mime_type = Column(String(128), nullable=True)
    uploader_name = Column(String(64), nullable=True)
    remark = Column(String(255), nullable=True)
    created_at = Column(DateTime, default=china_now_naive, server_default=func.now())


class UserOperationLog(Base):
    __tablename__ = "user_operation_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, nullable=True, index=True)
    username = Column(String(64), nullable=False, index=True)
    real_name = Column(String(64), nullable=True)
    org_id = Column(Integer, nullable=True)
    org_name = Column(String(128), nullable=True)
    module = Column(String(64), nullable=True)
    menu = Column(String(64), nullable=True)
    action = Column(String(64), nullable=True)
    operation_content = Column(Text, nullable=False)
    operation_ip = Column(String(64), nullable=True)
    result = Column(String(16), nullable=False, server_default="成功")
    vehicle = Column(String(32), nullable=True)
    plate_color = Column(String(16), nullable=True)
    device_no = Column(String(64), nullable=True)
    source = Column(String(16), nullable=False, server_default="manual", index=True)
    created_at = Column(DateTime, nullable=False, default=china_now_naive, server_default=func.now(), index=True)


class VehicleFaultLive(Base):
    """实时故障车辆（来自 Redis QUEUE_GZM 的 LPOP 消费结果）。

    与 manual_fault_report（人工报障）和 jt_device_fault（0x0200 报警位）互补，
    供智慧看板"车辆运行状态"栏展示当前故障车辆汇总与近期列表。
    TTL 由 redis_queue_fault_ttl_hours 控制，超期自动清理。
    """

    __tablename__ = "vehicle_fault_live"
    id = Column(Integer, primary_key=True, autoincrement=True)
    device_no = Column(String(64), nullable=True, index=True)
    plate_no = Column(String(32), nullable=True, index=True)
    vehicle_id = Column(Integer, nullable=True, index=True)
    company_id = Column(Integer, nullable=True, index=True)
    fault_code = Column(String(64), nullable=True)
    fault_level = Column(String(16), nullable=True, index=True)
    report_time = Column(DateTime, nullable=True, index=True)
    handled = Column(Boolean, nullable=False, server_default="0", index=True)
    raw = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=china_now_naive, server_default=func.now(), index=True)


class ObdEnergySnapshot(Base):
    """OBD 能耗快照（来自 Redis QUEUE_OBD_YC/QUEUE_OBD_DC 的 LPOP 消费结果）。

    同设备同日只保留最新一条（device_no + day + energy_type 唯一约束），
    供智慧看板"油耗/电耗统计"展示今日能耗、百公里能耗与近 7 日走势。
    """

    __tablename__ = "obd_energy_snapshot"
    __table_args__ = (
        UniqueConstraint("device_no", "day", "energy_type", name="uq_obd_energy_dev_day_type"),
    )
    id = Column(Integer, primary_key=True, autoincrement=True)
    device_no = Column(String(64), nullable=True, index=True)
    energy_type = Column(String(4), nullable=False, index=True)  # oil / ev
    fuel = Column(Float, nullable=True)        # 油车：升；电车：kWh（统一存 fuel 列）
    mileage = Column(Float, nullable=True)     # km
    report_time = Column(DateTime, nullable=True, index=True)
    day = Column(String(8), nullable=True, index=True)  # yyyyMMdd
    raw = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=china_now_naive, server_default=func.now(), index=True)
