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


class SysRole(Base):
    __tablename__ = "sys_role"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False)
    code = Column(String(64), unique=True)
    remark = Column(String(512))
    org_id = Column(Integer, ForeignKey("org_company.id"), nullable=True)
    is_global = Column(Boolean, nullable=False, default=False, server_default="0")
    permissions = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

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
    role_id = Column(Integer, ForeignKey("sys_role.id"))
    org_id = Column(Integer, ForeignKey("org_company.id"))
    jt808_user_id = Column(String(36), nullable=True, index=True)
    allow_pwd_edit = Column(Boolean, default=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
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
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

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
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

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
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    company = relationship("OrgCompany", back_populates="fleets")


class Driver(Base):
    __tablename__ = "driver"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False)
    company_id = Column(
        Integer, ForeignKey("org_company.id", ondelete="RESTRICT"), nullable=True, index=True
    )
    gender = Column(String(8), nullable=True)
    id_card = Column(String(32), nullable=True)
    phone = Column(String(32), nullable=True)
    birth_date = Column(Date, nullable=True)
    driver_license_no = Column(String(64), nullable=True)
    remark = Column(String(256))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    company = relationship("OrgCompany", backref="drivers")
    vehicles = relationship("Vehicle", back_populates="driver")


class Vehicle(Base):
    __tablename__ = "vehicle"
    id = Column(Integer, primary_key=True, autoincrement=True)
    plate_no = Column(String(16), nullable=False, unique=True, index=True)
    plate_color = Column(String(16), default="黄牌")
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
    manufacturer = Column(String(64))
    brand = Column(String(64))
    model = Column(String(64))
    vehicle_grade = Column(String(32))
    vehicle_usage = Column(String(64))
    speed_limit = Column(Numeric(10, 2), default=0)
    track_retain_days = Column(Integer, default=0)
    mileage_factor = Column(Numeric(6, 2))
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
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

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
    is_main = Column(Boolean, default=True)
    channel_no = Column(Integer, default=1)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    vehicle = relationship("Vehicle", back_populates="devices")


class ReserveTerminal(Base):
    __tablename__ = "reserve_terminal"
    id = Column(Integer, primary_key=True, autoincrement=True)
    terminal_id = Column(String(64), unique=True, index=True)
    first_auth_at = Column(DateTime(timezone=True), server_default=func.now())
    last_auth_at = Column(DateTime(timezone=True), server_default=func.now())
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
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

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
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class FaultTypeDict(Base):
    """基础数据：故障类型字典，供人工报障等业务选用。"""

    __tablename__ = "fault_type_dict"
    id = Column(Integer, primary_key=True, autoincrement=True)
    type_code = Column(String(32), nullable=False, unique=True, index=True)
    type_name = Column(String(64), nullable=False, index=True)
    description = Column(Text, nullable=True)
    fault_level = Column(String(16), nullable=False, server_default="中")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


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
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


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
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


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
    remark = Column(String(255))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

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
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

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
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    company = relationship("OrgCompany", backref="map_rule_categories")
    weather_rule = relationship("PrivateMapRuleWeather")


class UserLoginLog(Base):
    __tablename__ = "user_login_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(64), nullable=False, index=True)
    login_ip = Column(String(64), nullable=False, server_default="")
    login_at = Column(DateTime, nullable=False, server_default=func.now(), index=True)


class UserOperationLog(Base):
    __tablename__ = "user_operation_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(64), nullable=False, index=True)
    operation_content = Column(Text, nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.now(), index=True)
