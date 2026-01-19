import json
import os
import logging
import threading
import time
import shutil
from datetime import datetime, timedelta
from collections import defaultdict
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, filters, ContextTypes

from telegram.error import BadRequest

async def safe_edit(query, text, reply_markup=None):
    try:
        if query.message and query.message.text != text:
            await query.edit_message_text(text=text, reply_markup=reply_markup)
    except BadRequest:
        pass


# إعدادات البوت
# إعدادات البوت
# إعدادات البوت
TOKEN = "8450413524:AAE3Hxcb0tijnwb75kLJzkyhqIzPPBT8XYk"
ADMIN_ID = 8117492678
BOT_CHANNEL = "@TUX3T"
DATA_FILE = "data.json"
USERS_FILE = "users.json"

# نظام التحديثات الدقيقة للباك أب
BACKUP_INTERVAL = 1800  # كل 60 ثانية (دقيقة واحدة)
_last_backup_time = 0

# ========== المسارات المحلية على الهاتف ==========
BOT_DIR = "/storage/emulated/0/بو"
DATA_FILE = os.path.join(BOT_DIR, "data.json")
USERS_FILE = os.path.join(BOT_DIR, "users.json")
BACKUP_DIR = os.path.join(BOT_DIR, "backups")

# تأكد من وجود المجلدات
os.makedirs(BOT_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)

# إعدادات متقدمة
CACHE_TTL = 30
ACTION_COOLDOWNS = {
    "join_channel": 10,
    "verify_channel": 5,
    "daily_gift": 1,
    "store": 2,
    "admin": 0.5,
    "general": 1
}

# نظام Logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('bot_debug.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# أنظمة التخزين
_data_cache = {}
_cache_lock = threading.Lock()
_file_locks = {
    USERS_FILE: threading.Lock(),
    DATA_FILE: threading.Lock()
}
_cache_last_update = {}

# ===================== مدير Cooldown المحسن =====================

class CooldownManager:
    """مدير Cooldown محسن"""
    
    def __init__(self):
        self.cooldowns = defaultdict(dict)
        self.transaction_ids = set()
        self.lock = threading.Lock()
    
    def can_proceed(self, user_id, action_type, transaction_id=None):
        """التحقق من إمكانية التنفيذ"""
        user_id = str(user_id)
        
        with self.lock:
            # التحقق من تكرار المعاملة
            if transaction_id and transaction_id in self.transaction_ids:
                return False, 0, "معاملة مكررة"
            
            current_time = time.time()
            
            if user_id in self.cooldowns and action_type in self.cooldowns[user_id]:
                last_time = self.cooldowns[user_id][action_type]
                cooldown = ACTION_COOLDOWNS.get(action_type, 2)
                
                if current_time - last_time < cooldown:
                    remaining = cooldown - (current_time - last_time)
                    return False, remaining, "في فترة انتظار"
            
            # تسجيل الوقت والمعاملة
            self.cooldowns[user_id][action_type] = current_time
            if transaction_id:
                self.transaction_ids.add(transaction_id)
            
            return True, 0, "يمكن المتابعة"
    
    def clear_old_transactions(self):
        """تنظيف المعاملات القديمة"""
        with self.lock:
            # تنظيف بعد 24 ساعة
            current_time = time.time()
            self.transaction_ids = {tid for tid in self.transaction_ids 
                                  if not tid.startswith('tx_') or 
                                  current_time - int(tid.split('_')[-1]) / 1000 < 86400}
    
    def mark_transaction_complete(self, transaction_id):
        """تحديد المعاملة كمكتملة"""
        with self.lock:
            self.transaction_ids.discard(transaction_id)

cooldown_manager = CooldownManager()

# ===================== أقفال للمستخدمين والعمليات =====================

_user_locks = {}
_point_locks = {}
_verify_locks = {}
_daily_locks = {}
_store_locks = {}

# ===================== وظائف التخزين المحسنة =====================

def get_data_with_cache(cache_key, load_func, file_lock, ttl=CACHE_TTL):
    """تحميل البيانات مع التخزين المؤقت المحسن"""
    current_time = time.time()
    
    with _cache_lock:
        if (cache_key in _data_cache and 
            (current_time - _cache_last_update.get(cache_key, 0) < ttl)):
            return _data_cache[cache_key].copy()
    
    with file_lock:
        data = load_func()
        
        with _cache_lock:
            _data_cache[cache_key] = data.copy()
            _cache_last_update[cache_key] = current_time
        
        return data

def _load_users_from_file():
    """تحميل المستخدمين من الملف (داخلية)"""
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"خطأ في تحميل المستخدمين: {e}")
            return {}
    return {}

def load_users(force_reload=False):
    """تحميل بيانات المستخدمين"""
    if not force_reload:
        return get_data_with_cache("users", _load_users_from_file, _file_locks[USERS_FILE])
    else:
        return _load_users_from_file()

def _load_data_from_file():
    """تحميل البيانات من الملف (داخلية)"""
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"خطأ JSON في ملف البيانات: {e}")
            return create_initial_data()
        except Exception as e:
            logger.error(f"خطأ في تحميل البيانات: {e}")
            return create_initial_data()
    return create_initial_data()

def load_data(force_reload=False):
    """تحميل البيانات العامة"""
    if not force_reload:
        return get_data_with_cache("data", _load_data_from_file, _file_locks[DATA_FILE])
    else:
        return _load_data_from_file()

def save_users(users_data, backup=False):
    """حفظ بيانات المستخدمين"""
    with _file_locks[USERS_FILE]:
        try:
            if backup and os.path.exists(USERS_FILE):
                backup_file = f"backups/{USERS_FILE}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
                try:
                    shutil.copy2(USERS_FILE, backup_file)
                except Exception as e:
                    logger.error(f"خطأ في إنشاء backup: {e}")
            
            with open(USERS_FILE, 'w', encoding='utf-8') as f:
                json.dump(users_data, f, ensure_ascii=False, indent=4)
            
            with _cache_lock:
                _data_cache["users"] = users_data.copy()
                _cache_last_update["users"] = time.time()
            
            return True
        except Exception as e:
            logger.error(f"خطأ في حفظ المستخدمين: {e}")
            return False

def save_data(data, backup=False):
    """حفظ البيانات العامة"""
    with _file_locks[DATA_FILE]:
        try:
            if backup and os.path.exists(DATA_FILE):
                backup_file = f"backups/{DATA_FILE}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
                try:
                    shutil.copy2(DATA_FILE, backup_file)
                except Exception as e:
                    logger.error(f"خطأ في إنشاء backup للبيانات: {e}")
            
            with open(DATA_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            with _cache_lock:
                _data_cache["data"] = data.copy()
                _cache_last_update["data"] = time.time()
            
            return True
        except Exception as e:
            logger.error(f"خطأ في حفظ البيانات: {e}")
            return False

def create_initial_data():
    """إنشاء البيانات الأولية"""
    return {
        "channels": {},
        "codes": {},
        "reports": {},
        "admins": [str(ADMIN_ID)],
        "banned_users": [],
        "muted_users": {},
        "force_sub_channels": [],
        "stats": {
            "total_users": 0,
            "total_points": 0,
            "total_invites": 0,
            "total_purchases": 0,
            "total_joins": 0,
            "total_reports": 0,
            "total_daily_gifts": 0,
            "total_mutes": 0
        }
    }

def create_default_user_data():
    """إنشاء بيانات المستخدم الافتراضية"""
    return {
        "points": 0,
        "invites": 0,
        "invited_users": [],
        "bought_channels": {},
        "joined_channels": {},
        "username": "",
        "first_name": "",
        "last_name": "",
        "first_join": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_earned": 0,
        "total_spent": 0,
        "orders": [],
        "reports_made": 0,
        "reports_received": 0,
        "last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "active_subscriptions": [],
        "daily_gift": {
            "last_claimed": None,
            "streak": 0,
            "total_claimed": 0
        },
        "reported_channels": [],
        "inactive": False,
        "left_channels": [],
        "transactions": [],
        "temp_left_channels": [],  # قنوات غادرها مؤقتاً (قيد التجميع)
        "permanent_left_channels": [],  # قنوات غادرها نهائياً
        "left_completed_channels": []  # قنوات غادرها بعد اكتمالها
    }

def ensure_user_data_fields(user_data):
    """تأكد من وجود جميع الحقول المطلوبة"""
    default_fields = {
        "points": 0,
        "invites": 0,
        "total_earned": 0,
        "total_spent": 0,
        "left_channels": [],
        "transactions": [],
        "temp_left_channels": [],
        "permanent_left_channels": [],
        "left_completed_channels": []
    }
    
    for field, default_value in default_fields.items():
        if field not in user_data:
            user_data[field] = default_value
    
    for field, default_value in default_fields.items():
        if field not in user_data:
            user_data[field] = default_value

def get_user_data(user_id, force_reload=False):
    """الحصول على بيانات المستخدم"""
    users_data = load_users(force_reload)
    user_id = str(user_id)
    
    if user_id not in users_data:
        default_data = create_default_user_data()
        users_data[user_id] = default_data
        update_system_stats("total_users", increment=1)
        save_users(users_data, backup=False)
    
    user_data = users_data[user_id]
    ensure_user_data_fields(user_data)
    
    return user_data.copy()

def update_user_data(user_id, updates, action_type=None, transaction_id=None):
    """تحديث بيانات المستخدم"""
    user_id = str(user_id)
    
    # قفل المستخدم لمنع التضارب
    if user_id not in _user_locks:
        _user_locks[user_id] = threading.Lock()
    
    with _user_locks[user_id]:
        users_data = load_users(force_reload=True)
        
        if user_id not in users_data:
            users_data[user_id] = create_default_user_data()
        
        # تحديث الحقول
        for key, value in updates.items():
            users_data[user_id][key] = value
        
        # تحديث وقت النشاط
        users_data[user_id]["last_active"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        users_data[user_id]["inactive"] = False
        
        # تسجيل المعاملة إذا كانت متاحة
        if action_type and transaction_id:
            transaction = {
                "id": transaction_id,
                "action": action_type,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "updates": updates
            }
            users_data[user_id].setdefault("transactions", []).append(transaction)
        
        # حفظ مع قفل الملف
        if save_users(users_data, backup=False):
            return True
        return False

def update_system_stats(stat_key, increment=1, points=0):
    """تحديث إحصائيات النظام"""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            data = load_data(force_reload=True)
            stats = data.get("stats", {})
            
            if stat_key in stats:
                stats[stat_key] = stats.get(stat_key, 0) + increment
            else:
                stats[stat_key] = increment
            
            if points > 0:
                stats["total_points"] = stats.get("total_points", 0) + points
            
            data["stats"] = stats
            
            if save_data(data, backup=False):
                return True
                
        except Exception as e:
            logger.error(f"خطأ في تحديث الإحصائيات: {e}")
            if attempt == max_retries - 1:
                return False
            time.sleep(0.1)
    
    return False

# ===================== نظام إدارة النقاط المحسن =====================

def safe_add_points(user_id, points, operation="add", action_type=None, transaction_id=None):
    """إضافة/خصم نقاط - نسخة آمنة (تسمح بالنقاط السالبة)"""
    user_id = str(user_id)
    transaction_id = transaction_id or f"tx_{user_id}_{int(time.time() * 1000)}"
    
    # التحقق من التكرار باستخدام معرّف المعاملة
    user_data = get_user_data(user_id, force_reload=True)
    existing_tx = [t for t in user_data.get("transactions", []) if t.get("id") == transaction_id]
    if existing_tx:
        logger.warning(f"المعاملة مكررة: {transaction_id} للمستخدم {user_id}")
        return False, "معاملة مكررة"
    
    # قفل للمستخدم لمنع التضارب
    user_lock_key = f"points_{user_id}"
    _point_locks.setdefault(user_lock_key, threading.Lock())
    
    with _point_locks[user_lock_key]:
        user_data = get_user_data(user_id, force_reload=True)
        current_points = user_data.get("points", 0)
        
        if operation == "add":
            new_points = current_points + points
            total_earned = user_data.get("total_earned", 0) + points
            
            updates = {
                "points": new_points,
                "total_earned": total_earned
            }
            
            if update_user_data(user_id, updates, action_type or "add_points", transaction_id):
                # تحديث الإحصائيات مرة واحدة فقط
                if action_type != "stats_update":
                    update_system_stats("total_points", points=points)
                return True, "تمت الإضافة بنجاح"
                
        elif operation == "subtract":
            # ✅ **الإصلاح: السماح بالنقاط السالبة**
            new_points = current_points - points
            total_spent = user_data.get("total_spent", 0) + points
            
            updates = {
                "points": new_points,  # يمكن أن تكون سالبة
                "total_spent": total_spent
            }
            
            if update_user_data(user_id, updates, action_type or "subtract_points", transaction_id):
                logger.info(f"💸 خصم {points} نقطة من {user_id}: {current_points} → {new_points}")
                
                # ✅ **إصلاح إحصائيات النقاط السالبة**
                if points > 0 and action_type != "stats_update":
                    # عند الخصم، نخصم النقاط من الإحصائيات الإجمالية
                    update_system_stats("total_points", points=-points)
                
                return True, "تم الخصم بنجاح"
        
        return False, "خطأ غير معروف"

# ===================== نظام الكتم =====================

def is_muted(user_id):
    """التحقق مما إذا كان المستخدم مكتوماً"""
    data = load_data()
    user_id = str(user_id)
    
    if user_id in data.get("muted_users", {}):
        mute_data = data["muted_users"][user_id]
        mute_until = mute_data.get("until")
        
        if mute_until:
            try:
                mute_until_time = datetime.strptime(mute_until, "%Y-%m-%d %H:%M:%S")
                if datetime.now() < mute_until_time:
                    return True, mute_until
                else:
                    del data["muted_users"][user_id]
                    save_data(data)
                    return False, None
            except Exception as e:
                logger.error(f"خطأ في معالجة وقت الكتم: {e}")
                return False, None
        return True, "دائم"
    
    return False, None

def add_muted_user(user_id, mute_duration=None, reason=""):
    """إضافة مستخدم مكتوم"""
    data = load_data()
    user_id = str(user_id)
    
    mute_info = {
        "muted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "reason": reason,
        "muted_by": ADMIN_ID
    }
    
    if mute_duration:
        mute_until = datetime.now() + timedelta(seconds=mute_duration)
        mute_info["until"] = mute_until.strftime("%Y-%m-%d %H:%M:%S")
        mute_info["duration"] = mute_duration
    
    data["muted_users"][user_id] = mute_info
    data["stats"]["total_mutes"] = data["stats"].get("total_mutes", 0) + 1
    save_data(data)
    
    return mute_info

def remove_muted_user(user_id):
    """إزالة مستخدم من قائمة المكتومين"""
    data = load_data()
    user_id = str(user_id)
    
    if user_id in data.get("muted_users", {}):
        del data["muted_users"][user_id]
        if save_data(data):
            return True
    
    return False

def cleanup_expired_mutes(context: ContextTypes.DEFAULT_TYPE = None):
    """تنظيف الكتم المنتهي"""
    try:
        data = load_data()
        muted_users = data.get("muted_users", {})
        removed_count = 0
        
        if isinstance(muted_users, dict):
            for user_id, mute_data in list(muted_users.items()):
                mute_until = mute_data.get("until")
                if mute_until:
                    try:
                        mute_until_time = datetime.strptime(mute_until, "%Y-%m-%d %H:%M:%S")
                        if datetime.now() >= mute_until_time:
                            del data["muted_users"][user_id]
                            removed_count += 1
                    except Exception as e:
                        logger.error(f"خطأ في تنظيف الكتم: {e}")
        
        if removed_count > 0:
            save_data(data)
            logger.info(f"🧹 تم تنظيف {removed_count} مستخدم منتهي الكتم")
    
    except Exception as e:
        logger.error(f"خطأ في cleanup_expired_mutes: {e}")

def format_time(seconds):
    """تحويل الثواني إلى نص مقروء"""
    if seconds == 0:
        return "دائم"
    
    days = seconds // (24 * 3600)
    seconds %= (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    
    result = []
    if days > 0:
        result.append(f"{days} يوم")
    if hours > 0:
        result.append(f"{hours} ساعة")
    if minutes > 0:
        result.append(f"{minutes} دقيقة")
    if seconds > 0:
        result.append(f"{seconds} ثانية")
    
    return " و ".join(result) if result else "0 ثانية"

# ===================== وظائف مساعدة محسنة =====================

def is_admin(user_id):
    """التحقق مما إذا كان المستخدم أدمن"""
    data = load_data()
    return str(user_id) in data.get("admins", [str(ADMIN_ID)])

def is_banned(user_id):
    """التحقق مما إذا كان المستخدم محظور"""
    data = load_data()
    return str(user_id) in data.get("banned_users", [])

def find_user_by_username(username):
    """الببحث عن مستخدم باليوزر"""
    users_data = load_users()
    username = username.replace("@", "").lower()
    
    for uid, user_data in users_data.items():
        if user_data.get("username", "").lower() == username:
            return uid
    return None

async def send_to_admin(bot, message):
    """إرسال رسالة للمالك"""
    try:
        await bot.send_message(ADMIN_ID, message, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Error sending to admin: {e}")

async def check_channel_subscription(bot, user_id, channel_username):
    """التحقق من اشتراك المستخدم في قناة"""
    try:
        channel_username = channel_username.replace("@", "").strip()
        
        try:
            chat = await bot.get_chat(chat_id=f"@{channel_username}")
        except Exception as chat_error:
            logger.error(f"خطأ في جلب معلومات القناة: {chat_error}")
            return None
        
        try:
            member = await bot.get_chat_member(
                chat_id=chat.id,
                user_id=user_id
            )
            
            valid_statuses = ["member", "administrator", "creator"]
            
            if member.status in valid_statuses:
                return True
            else:
                return False
                
        except Exception as member_error:
            error_text = str(member_error).lower()
            
            if "user not found" in error_text or "user not participant" in error_text:
                return False
            elif "forbidden" in error_text or "kicked" in error_text:
                return None
            else:
                logger.error(f"خطأ في التحقق من العضوية: {member_error}")
                return None
                
    except Exception as e:
        logger.error(f"خطأ عام في التحقق من الاشتراك: {e}")
        return None

async def check_bot_is_admin(bot, channel_username):
    """التحقق مما إذا كان البوت مشرفاً في قناة"""
    try:
        channel_username = channel_username.replace("@", "").strip()
        
        try:
            chat = await bot.get_chat(chat_id=f"@{channel_username}")
            
            bot_member = await bot.get_chat_member(
                chat_id=chat.id,
                user_id=(await bot.get_me()).id
            )
            
            if bot_member.status in ("administrator", "creator"):
                return True
            else:
                return False

        except Exception as e:
            logger.error(f"خطأ في التحقق من إشراف البوت: {e}")
            return False

    except Exception as e:
        logger.error(f"خطأ عام في التحقق من إشراف البوت: {e}")
        return False

def check_user_channel_status(user_id, channel_id):
    """فحص شامل ودقيق لحالة المستخدم في القناة"""
    user_data = get_user_data(user_id, force_reload=True)
    channel_data = load_data().get("channels", {}).get(channel_id, {})
    
    # التحقق من أن القناة موجودة
    if not channel_data:
        return "not_found"
    
    # تحقق مما إذا كانت القناة مكتملة
    is_completed = channel_data.get("completed", False)
    
    # 1. التحقق من القنوات النشطة
    active_subs = user_data.get("active_subscriptions", [])
    if channel_id in active_subs:
        joined_channels = user_data.get("joined_channels", {})
        join_data = joined_channels.get(channel_id, {})
        
        if join_data.get("verified", False) and not join_data.get("left", False):
            # إذا كانت القناة مكتملة وهو منضم لها
            if is_completed:
                return "joined_completed"  # كان منضماً ثم اكتملت القناة
            return "joined_active"
        else:
            # تصحيح البيانات غير المتسقة
            updates = {
                "active_subscriptions": [c for c in active_subs if c != channel_id]
            }
            update_user_data(user_id, updates, "fix_active_subscriptions")
    
    # 2. التحقق من joined_channels
    joined_channels = user_data.get("joined_channels", {})
    if channel_id in joined_channels:
        join_data = joined_channels[channel_id]
        
        # إذا كان مغادراً
        if join_data.get("left", False):
            # تحقق إذا كانت القناة مكتملة
            if is_completed:
                return "left_completed"  # غادر قناة ثم اكتملت - لا ترجع أبداً
            else:
                return "left_active"  # غادر قناة قيد التجميع - ترجع
    
    # 3. التحقق من القنوات المتروكة نهائياً
    permanent_left = user_data.get("permanent_left_channels", [])
    if channel_id in permanent_left:
        return "permanent_left"
    
    # 4. التحقق من القنوات المتروكة مؤقتاً
    temp_left = user_data.get("temp_left_channels", [])
    if channel_id in temp_left:
        return "temp_left"
    
    # 5. القنوات المتروكة القديمة (للتوافق)
    left_channels = user_data.get("left_channels", [])
    if channel_id in left_channels:
        return "temp_left"  # نعتبرها مؤقتة للتوافق مع البيانات القديمة
    
    return "not_joined"

def can_user_join_reactivated_channel(user_id, channel_id, channel_data):
    """التحقق من إمكانية الانضمام للقناة المُعاد تفعيلها"""
    user_id = str(user_id)
    
    # إذا كان المستخدم صاحب القناة
    if user_id == channel_data.get("owner"):
        return False, "لا يمكنك الانضمام لقناتك!"
    
    user_data = get_user_data(user_id, force_reload=True)
    joined_channels = user_data.get("joined_channels", {})
    
    # إذا لم ينضم للقناة من قبل
    if channel_id not in joined_channels:
        return True, ""
    
    join_info = joined_channels[channel_id]
    current_round = channel_data.get("reuse_count", 0)
    user_round = join_info.get("round", 0)
    
    # 🔴 🔴 🔴 **الإصلاح الرئيسي هنا** 🔴 🔴 🔴
    # إذا كانت القناة أعيد تفعيلها (جولة جديدة)
    if current_round > user_round:
        return True, ""
    
    # نفس الجولة
    if user_round == current_round:
        if join_info.get("verified", False) and not join_info.get("left", False):
            return False, "لقد انضممت لهذه القناة مسبقاً!"
        if join_info.get("left", False):
            return False, "غادرت هذه القناة في هذه الدورة!"
    
    # حالة left_completed (القناة المكتملة السابقة)
    if join_info.get("left_completed", False):
        completed_round = join_info.get("completed_round", 0)
        
        # إذا كانت الجولة الحالية أكبر من الجولة المكتملة → جولة جديدة
        if current_round > completed_round:
            return True, ""
        else:
            return False, "لا يمكنك الانضمام لنفس الجولة المكتملة!"
    
    return True, ""
 
def can_user_join_channel(user_id, channel_id, channel_username, channel_data=None):
    """التحقق النهائي مما إذا كان يمكن للمستخدم الانضمام للقناة - مُحسنة"""
    
    if channel_data is None:
        data = load_data()
        channel_data = data.get("channels", {}).get(channel_id, {})

    # القناة غير موجودة
    if not channel_data:
        return False, "القناة غير موجودة!"

    # المستخدم صاحب القناة
    if str(user_id) == channel_data.get("owner"):
        return False, "لا يمكنك الانضمام لقناتك!"

    # رقم دورة التجميع الحالية
    current_round = channel_data.get("reuse_count", 0)

    user_data = get_user_data(user_id, force_reload=True)
    joined_channels = user_data.get("joined_channels", {})

    # 🔴 إذا المستخدم انضم سابقاً
    if channel_id in joined_channels:
        join_info = joined_channels[channel_id]

        user_round = join_info.get("round", 0)
        left = join_info.get("left", False)
        verified = join_info.get("verified", False)

        # ✅ التحقق من حالة القناة المكتملة التي غادرها
        left_completed = join_info.get("left_completed", False)
        
        if left_completed:
            # ✅ إذا غادر قناة مكتملة وهناك جولة جديدة
            completed_round = join_info.get("completed_round", 0)
            
            if current_round > completed_round:
                # 🟢 جولة جديدة - يمكنه الانضمام
                return True, "يمكنك الانضمام للجولة الجديدة!"
            else:
                # 🔴 نفس الجولة المكتملة - لا يمكن
                return False, "لا يمكنك الانضمام لنفس الجولة المكتملة!"
        
        # ✅ نفس دورة التجميع
        if user_round == current_round:
            if verified and not left:
                return False, "لقد انضممت لهذه القناة مسبقاً!"
            if left:
                # 🔥 الإصلاح الرئيسي: السماح بالعودة!
                return True, ""  # ✅ يمكنه العودة!

        # 🟢 دورة جديدة → يسمح له
        if user_round < current_round:
            return True, ""

    # 🔴 القناة مكتملة ولا توجد دورة جديدة
    if channel_data.get("completed", False):
        return False, "هذه القناة مكتملة حالياً!"

    # 🟢 لم ينضم أبداً
    return True, ""

def cleanup_old_left_completed_flags():
    """تنظيف علامات left_completed القديمة للقنوات المحذوفة أو المعاد تفعيلها"""
    try:
        users_data = load_users()
        data = load_data()
        cleaned = 0
        
        for user_id, user_data in users_data.items():
            if "joined_channels" not in user_data:
                continue
                
            for channel_id, join_info in list(user_data["joined_channels"].items()):
                if join_info.get("left_completed", False):
                    # إذا كانت القناة غير موجودة أو أعيد تفعيلها
                    channel_data = data.get("channels", {}).get(channel_id)
                    
                    if not channel_data:
                        # قناة محذوفة - إزالة العلامة
                        del user_data["joined_channels"][channel_id]
                        cleaned += 1
                    elif not channel_data.get("completed", False):
                        # قناة أعيد تفعيلها - إزالة العلامة
                        join_info["left_completed"] = False
                        if "completed_round" in join_info:
                            del join_info["completed_round"]
                        cleaned += 1
        
        if cleaned > 0:
            save_users(users_data)
            logger.info(f"🧹 تم تنظيف {cleaned} علامة left_completed قديمة")
        
        return cleaned
    except Exception as e:
        logger.error(f"❌ خطأ في cleanup_old_left_completed_flags: {e}")
        return 0
        
async def can_claim_daily_gift(user_id):
    """التحقق مما إذا كان يمكن للمستخدم المطالبة بالهدية اليومية"""
    user_data = get_user_data(user_id)
    daily_gift = user_data.get("daily_gift", {})
    last_claimed = daily_gift.get("last_claimed")
    
    if not last_claimed:
        return True, 0
    
    try:
        last_claimed_date = datetime.strptime(last_claimed, "%Y-%m-%d %H:%M:%S")
        now = datetime.now()
        
        if now - last_claimed_date >= timedelta(hours=24):
            return True, 0
        else:
            next_claim = last_claimed_date + timedelta(hours=24)
            remaining = next_claim - now
            hours = int(remaining.total_seconds() // 3600)
            minutes = int((remaining.total_seconds() % 3600) // 60)
            return False, f"{hours}:{minutes:02d}"
    except Exception as e:
        logger.error(f"خطأ في التحقق من الهدية اليومية: {e}")
        return True, 0

async def check_force_subscription(bot, user_id, chat_id=None):
    """التحقق من اشتراك المستخدم في جميع القنوات الإجبارية"""
    data = load_data()
    force_channels = data.get("force_sub_channels", [])
    
    if not force_channels:
        return True, []
    
    not_subscribed = []
    
    for channel_username in force_channels:
        bot_is_admin = await check_bot_is_admin(bot, channel_username)
        
        if not bot_is_admin:
            continue
        
        is_subscribed = await check_channel_subscription(bot, user_id, channel_username)
        
        if is_subscribed is False:
            not_subscribed.append(channel_username)
    
    if not_subscribed:
        return False, not_subscribed
    
    return True, []

async def check_and_enforce_subscription(bot, user_id, chat_id, context):
    """التحقق من الاشتراك الإجباري وإنفاذه"""
    can_use, missing_channels = await check_force_subscription(bot, user_id, chat_id)
    
    if not can_use:
        keyboard = []
        for channel in missing_channels:
            keyboard.append([
                InlineKeyboardButton(
                    f"📢 @{channel}", 
                    url=f"https://t.me/{channel.replace('@', '')}"
                )
            ])
        
        keyboard.append([
            InlineKeyboardButton("✅ تحقق من الاشتراك", callback_data="check_force_sub")
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message = f"🚫 يجب الاشتراك في القنوات التالية أولاً!\n\n"
        
        for i, channel in enumerate(missing_channels, 1):
            message += f"{i}. @{channel}\n"
        
        message += "\n✅ بعد الاشتراك، اضغط زر التحقق بالأسفل."
        
        if context.user_data.get('last_force_sub_message_id'):
            try:
                await bot.delete_message(chat_id, context.user_data['last_force_sub_message_id'])
            except:
                pass
        
        sent_msg = await bot.send_message(
            chat_id,
            message,
            reply_markup=reply_markup,
            parse_mode="HTML"
        )
        context.user_data['last_force_sub_message_id'] = sent_msg.message_id
        
        return False
    return True

def can_user_report_channel(user_id, channel_id):
    """التحقق مما إذا كان يمكن للمستخدم الإبلاغ عن قناة"""
    user_data = get_user_data(user_id)
    reported_channels = user_data.get("reported_channels", [])
    
    return channel_id not in reported_channels

def add_user_reported_channel(user_id, channel_id):
    """إضافة قناة إلى قائمة القنوات المبلغ عنها من قبل المستخدم"""
    user_data = get_user_data(user_id)
    reported_channels = user_data.get("reported_channels", [])
    
    if channel_id not in reported_channels:
        reported_channels.append(channel_id)
        updates = {
            "reported_channels": reported_channels,
            "reports_made": user_data.get("reports_made", 0) + 1
        }
        if update_user_data(user_id, updates, "report_channel"):
            return True
    return False

# ===================== دوال الإشعارات الجديدة =====================

async def send_join_notification_to_owner(bot, channel_username, owner_id, user_data, is_returning_user, current_count, required_count, current_round):
    """إرسال إشعار انضمام لصاحب القناة"""
    try:
        if owner_id and owner_id != str(ADMIN_ID):
            status_text = "🔄 عودة" if is_returning_user else "🎉 انضمام جديد"
            
            await bot.send_message(
                int(owner_id),
                f"{status_text} لقناتك!\n\n"
                f"📢 @{channel_username}\n"
                f"👤 المستخدم: @{user_data.get('username', 'بدون يوزر')}\n"
                f"📊 العداد: {current_count - 1} → {current_count}/{required_count}\n"
                f"🔢 الجولة: {current_round + 1}",
                parse_mode="HTML"
            )
            logger.info(f"📤 تم إرسال إشعار انضمام لصاحب القناة: {owner_id}")
    except Exception as e:
        logger.error(f"❌ خطأ في إرسال إشعار الانضمام: {e}")

async def send_channel_completion_notifications(bot, channel_username, owner_id, current_count, required_count, created_at):
    """إرسال إشعارات اكتمال القناة"""
    try:
        # 1. إرسال إشعار لمالك القناة
        if owner_id and owner_id != str(ADMIN_ID):
            try:
                await bot.send_message(
                    int(owner_id),
                    f"🎉 مبروك! قناتك اكتملت!\n\n"
                    f"📢 @{channel_username}\n"
                    f"✅ العدد النهائي: {current_count}/{required_count}\n"
                    f"📅 تاريخ الاكتمال: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
                    f"💡 تم اكتمال القناة بنجاح!\n"
                    f"🔄 يمكنك شراء أعضاء جدد لإعادة تفعيلها",
                    parse_mode="HTML"
                )
                logger.info(f"📤 تم إرسال إشعار اكتمال لصاحب القناة: {owner_id}")
            except Exception as e:
                logger.error(f"❌ خطأ في إرسال إشعار الاكتمال للمالك {owner_id}: {e}")
        
        # 2. إرسال إشعار لمالك البوت
        try:
            # الحصول على بيانات المالك
            owner_username = "غير معروف"
            if owner_id != str(ADMIN_ID):
                owner_data = get_user_data(owner_id)
                owner_username = f"@{owner_data.get('username', owner_id)}"
            
            # حساب المدة
            duration = "غير معروف"
            if created_at:
                try:
                    start_time = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                    end_time = datetime.now()
                    diff = end_time - start_time
                    hours = diff.seconds // 3600
                    minutes = (diff.seconds % 3600) // 60
                    duration = f"{diff.days} يوم و {hours} ساعة و {minutes} دقيقة"
                except:
                    pass
            
            await bot.send_message(
                ADMIN_ID,
                f"🎯 قناة اكتملت الآن!\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📢 القناة: @{channel_username}\n"
                f"👤 المالك: {owner_username}\n"
                f"🆔 آيدي المالك: {owner_id}\n"
                f"📊 العدد النهائي: {current_count}/{required_count}\n"
                f"📅 وقت البدء: {created_at}\n"
                f"📅 وقت الاكتمال: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"⏰ المدة: {duration}\n"
                f"💰 النقاط المدفوعة: {required_count * 2}\n"
                f"━━━━━━━━━━━━━━━━━━━━",
                parse_mode="HTML"
            )
            logger.info(f"📤 تم إرسال إشعار اكتمال للمالك: @{channel_username}")
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال إشعار الاكتمال للمالك: {e}")
            
    except Exception as e:
        logger.error(f"❌ خطأ عام في إرسال إشعارات الاكتمال: {e}")

# ===================== معالجة الأوامر الرئيسية =====================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر /start مع نظام إشعارات شامل"""
    user = update.message.from_user
    user_id = str(user.id)
    
    # التحقق من الحظر
    if is_banned(user.id):
        await update.message.reply_text("❌ أنت محظور من استخدام البوت.")
        return
    
    # التحقق من الكتم
    is_user_muted, mute_until = is_muted(user_id)
    if is_user_muted:
        mute_time = mute_until if mute_until else "دائم"
        await update.message.reply_text(
            f"🔇 أنت مكتوم من استخدام البوت!\n\n"
            f"⏰ ينتهي الكتم في: {mute_time}\n\n"
            f"📞 للاستفسار تواصل مع الإدارة.",
            parse_mode="HTML"
        )
        return
    
    # التحقق من الاشتراك الإجباري
    can_use = await check_and_enforce_subscription(
        context.bot, 
        user.id, 
        update.message.chat_id,
        context
    )
    
    if not can_use:
        return
    
    # ✅✅✅ حفظ حالة المستخدم الجديد قبل أي تحديث ✅✅✅
    users_data = load_users()
    is_new_user = (user_id not in users_data)
    
    # 🔢 حساب رقم المستخدم الترتيبي (فقط للمستخدمين الجدد)
    user_number = len(users_data) + 1 if is_new_user else None
    
    # الآن يمكن تحديث البيانات
    user_data = get_user_data(user_id)
    
    updates = {
        "username": user.username or "",
        "first_name": user.first_name or "",
        "last_name": user.last_name or ""
    }
    update_user_data(user_id, updates, "user_info_update")
    
    # ✅✅✅ معالجة الإحالة - فقط للمستخدمين الجدد ✅✅✅
    if context.args and is_new_user:  # ✅ التحقق من المستخدم الجديد هنا مباشرة
        ref_id = context.args[0]
        
        # إعادة تحميل users_data للتأكد
        users_data = load_users()
        
        if ref_id != user_id and ref_id in users_data:
            # تحميل بيانات المُحيل
            ref_data = get_user_data(ref_id, force_reload=True)
            invited_users = ref_data.get("invited_users", [])
            
            # تأكد من أن المستخدم ليس في قائمة المدعوين
            if user_id not in invited_users:
                old_points = ref_data.get("points", 0)
                old_invites = ref_data.get("invites", 0)
                
                # تحديث بيانات المُحيل
                ref_data["invites"] = old_invites + 1
                ref_data["invited_users"] = invited_users + [user_id]
                
                # إضافة النقاط للمحيل
                success, message = safe_add_points(ref_id, 4, "add", "invite_points")
                if success:
                    new_points = old_points + 4
                    new_invites = old_invites + 1
                    
                    update_system_stats("total_invites", increment=1)
                    
                    # 🔔 1. إشعار لصاحب رابط الإحالة
                    try:
                        await context.bot.send_message(
                            int(ref_id),
                            f"🎉 شخص جديد دخل عبر رابط دعوتك!\n\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"👤 معلومات الشخص الجديد:\n"
                            f"• اليوزر: @{user.username or 'بدون يوزر'}\n"
                            f"• الآيدي: <code>{user_id}</code>\n"
                            f"• الاسم: {user.first_name} {user.last_name or ''}\n\n"
                            f"💰 مكافأتك:\n"
                            f"• حصلت على: 4 نقاط ✨\n"
                            f"• نقاطك قبل: {old_points}\n"
                            f"• نقاطك الآن: {new_points} 🎯\n\n"
                            f"🔗 إحصائياتك:\n"
                            f"• إجمالي دعواتك: {new_invites} شخص\n"
                            f"• أرباحك من الدعوات: {new_invites * 4} نقطة\n\n"
                            f"📅 الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n\n"
                            f"🚀 استمر في دعوة الأصدقاء لزيادة نقاطك!",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logger.error(f"خطأ في إرسال إشعار الإحالة لـ {ref_id}: {e}")
                    
                    # 🔔 2. إشعار لمالك البوت عن الإحالة
                    try:
                        ref_username = users_data[ref_id].get("username", "بدون يوزر")
                        ref_first_name = users_data[ref_id].get("first_name", "غير معروف")
                        
                        await context.bot.send_message(
                            ADMIN_ID,
                            f"🔗 إحالة جديدة في البوت!\n\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"👤 المُحيل:\n"
                            f"• الاسم: {ref_first_name}\n"
                            f"• اليوزر: @{ref_username}\n"
                            f"• الآيدي: <code>{ref_id}</code>\n"
                            f"• نقاطه قبل: {old_points}\n"
                            f"• نقاطه الآن: {new_points} (+4)\n"
                            f"• إجمالي دعواته: {new_invites} شخص\n\n"
                            f"👥 الشخص الجديد:\n"
                            f"• الاسم: {user.first_name} {user.last_name or ''}\n"
                            f"• اليوزر: @{user.username or 'بدون يوزر'}\n"
                            f"• الآيدي: <code>{user_id}</code>\n\n"
                            f"💰 المكافأة:\n"
                            f"• تم إضافة 4 نقاط للمُحيل ✅\n\n"
                            f"📅 التاريخ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                            f"━━━━━━━━━━━━━━━━━━━━",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logger.error(f"خطأ في إرسال إشعار الإحالة للمالك: {e}")
                else:
                    logger.error(f"❌ فشل إضافة نقاط الإحالة للمستخدم {ref_id}: {message}")
            else:
                logger.info(f"⚠️ المستخدم {user_id} موجود بالفعل في قائمة المدعوين للمُحيل {ref_id}")
        else:
            # حالات لا تستحق المكافأة
            if ref_id == user_id:
                logger.info(f"⚠️ المستخدم {user_id} حاول استخدام رابط دعوته الخاص")
            elif ref_id not in users_data:
                logger.info(f"⚠️ رابط إحالة غير صحيح: {ref_id}")
    elif context.args and not is_new_user:
        # المستخدم قديم يحاول استخدام رابط إحالة
        logger.info(f"⚠️ المستخدم {user_id} دخل عبر رابط إحالة ولكنه مستخدم قديم")
    
    # ✅ إشعار المالك عن المستخدم الجديد مع الرقم الترتيبي (فقط عند أول دخول)
    if is_new_user:
        # الحصول على إحصائيات
        stats = get_user_statistics()
        stats_text = ""
        if stats:
            # حساب نسبة النمو الصحيحة
            yesterday_users = user_number - stats.get('new_today', 0)
            growth_rate = (stats.get('new_today', 0) / max(1, yesterday_users)) * 100
            
            stats_text = (
                f"📊 إحصائيات البوت الحالية:\n"
                f"• إجمالي المستخدمين: {stats.get('total_users', 0)}\n"
                f"• المستخدمين النشطين اليوم: {stats.get('active_users', 0)}\n"
                f"• الجدد اليوم: {stats.get('new_today', 0)}\n"
                f"• الجدد الأسبوع: {stats.get('new_week', 0)}\n"
                f"• الجدد الشهر: {stats.get('new_month', 0)}\n"
                f"• نسبة النمو اليوم: {growth_rate:.1f}%\n"
                f"• المستخدمين باليوزر: {stats.get('with_username', 0)}\n"
                f"• المستخدمين بالدعوات: {stats.get('with_invites', 0)}\n"
                f"• النقاط الإجمالية: {stats.get('total_points', 0)}\n"
                f"• إجمالي الدعوات: {stats.get('total_invites', 0)}\n"
            )
        
        admin_msg = (
            f"👤 دخول جديد للبوت!\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🔢 رقم المستخدم: {user_number}\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"👤 يوزر: @{user.username or 'بدون'}\n"
            f"📛 الاسم: {user.first_name} {user.last_name or ''}\n"
            f"🌐 اللغة: {user.language_code or 'غير معروف'}\n"
            f"📅 التاريخ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{stats_text}"
            f"━━━━━━━━━━━━━━━━━━━━"
        )
        
        # إرسال الإشعار مرة واحدة فقط للمستخدمين الجدد
        await send_to_admin(context.bot, admin_msg)
    
    # رسالة الترحيب
    welcome_msg = (
        f"👋 أهلاً وسهلاً {user.first_name}!\n\n"
        f"🌟 مرحباً بك في بوت خدمات القنوات 🌟\n\n"
        f"📌 كيفية عمل البوت:\n"
        f"1️⃣ ادخل على المتجر واشترِ أعضاء لقناتك\n"
        f"2️⃣ شارك رابط دعوتك مع أصدقائك واحصل على نقاط\n"
        f"3️⃣ انضم للقنوات في قسم التجميع واحصل على نقاط\n"
        f"4️⃣ استخدم نقاطك لشراء أعضاء جدد\n\n"
        f"📢 قناة البوت الرسمية: {BOT_CHANNEL}\n"
        f"🎯 لديك: {user_data['points']} نقطة\n"
        f"🔗 دعوت: {user_data['invites']} شخص\n\n"
        f"اختر من القائمة:"
    )
    
    keyboard = [
        [InlineKeyboardButton("🛒 المتجر", callback_data="store")],
        [InlineKeyboardButton("📊 جمع النقاط", callback_data="collect_points")],
        [InlineKeyboardButton("🎁 الهدية اليومية", callback_data="daily_gift")],
        [InlineKeyboardButton("🏆 التوب", callback_data="top")],
        [InlineKeyboardButton("🔗 رابط الدعوة", callback_data="invite_link")],
        [InlineKeyboardButton("🎟️ الأكواد", callback_data="codes")],
    ]
    
    if is_admin(user.id):
        keyboard.append([InlineKeyboardButton("👑 لوحة الإدمن", callback_data="admin_panel")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(welcome_msg, reply_markup=reply_markup, parse_mode="HTML")


async def handle_code_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة أمر /code"""
    if len(context.args) < 1:
        await update.message.reply_text("📝 استخدام الكود:\n\n/code اسم_الكود\nمثال: /code TUX100", parse_mode="HTML")
        return
    
    code_name = context.args[0].upper()
    user_id = str(update.message.from_user.id)
    data = load_data()
    
    if code_name in data.get("codes", {}):
        code_data = data["codes"][code_name]
        
        if code_data.get("used_count", 0) >= code_data.get("max_uses", 0):
            await update.message.reply_text("❌ هذا الكود تم استخدامه بالكامل!")
            return
        
        if user_id in code_data.get("used_by", []):
            await update.message.reply_text("❌ لقد استخدمت هذا الكود من قبل!")
            return
        
        points = code_data.get("points", 0)
        
        success, message = safe_add_points(user_id, points, "add", "code_redeem")
        if not success:
            await update.message.reply_text(f"❌ {message}")
            return
        
        code_data["used_count"] = code_data.get("used_count", 0) + 1
        if "used_by" not in code_data:
            code_data["used_by"] = []
        code_data["used_by"].append(user_id)
        
        save_data(data)
        
        user_data = get_user_data(user_id)
        
        await update.message.reply_text(
            f"🎉 تم استخدام الكود بنجاح!\n\n"
            f"🎟️ الكود: {code_name}\n"
            f"💰 النقاط: {points}\n"
            f"🎯 نقاطك الآن: {user_data['points']}\n\n"
            f"📊 استخدامات الكود: {code_data['used_count']}/{code_data['max_uses']}",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text("❌ الكود غير صحيح!")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة ضغطات الأزرار"""
    user_data = context.user_data
    query = update.callback_query
    user_id = str(query.from_user.id)
    
    # التحقق من الحظر
    if is_banned(query.from_user.id):
        await query.answer("❌ أنت محظور من استخدام البوت.", show_alert=True)
        return
    
    # التحقق من الكتم
    is_user_muted, mute_until = is_muted(user_id)
    if is_user_muted:
        mute_time = mute_until if mute_until else "دائم"
        await query.answer(f"🔇 أنت مكتوم حتى: {mute_time}", show_alert=True)
        return
    
    # التحقق من الاشتراك الإجباري للأوامر الأساسية
    if query.data not in ["check_force_sub", "back_main", "admin_panel", "admin_storage_info", "refresh_storage_info"]:
        can_use = await check_and_enforce_subscription(
            context.bot, 
            int(user_id), 
            query.message.chat_id,
            context
        )
        
        if not can_use:
            return
    
    # التحقق من cooldown
    allowed, remaining, reason = cooldown_manager.can_proceed(user_id, "general")
    if not allowed:
        await query.answer(f"⏳ {reason}. انتظر {remaining:.1f} ثانية", show_alert=True)
        return
    
    try:
        await query.answer()
    except Exception:
        pass
    
    try:
        if query.data == "store":
            await show_store(query)
            
        elif query.data == "collect_points":
            await show_collect_points(query, user_id)
            
        elif query.data == "daily_gift":
            await show_daily_gift(query, user_id)
            
        elif query.data == "top":
            await show_top(query)
            
        elif query.data == "invite_link":
            await show_invite_link(query, user_id, context.bot)
            
        elif query.data == "codes":
            await show_codes_panel(query)
            
        elif query.data == "admin_panel":
            if is_admin(query.from_user.id):
                await show_admin_panel(query)
            else:
                await query.answer("❌ ليس لديك صلاحية الوصول!", show_alert=True)
        
        elif query.data == "back_main":
            await back_to_main(query, user_id)
            
        elif query.data == "claim_daily_gift":
            await handle_claim_daily_gift(query, user_id, context.bot)
            
        elif query.data == "check_force_sub":
            can_use, missing_channels = await check_force_subscription(
                context.bot,
                int(user_id),
                query.message.chat_id
            )
            
            if can_use:
                await query.answer("✅ أنت مشترك في جميع القنوات المطلوبة!", show_alert=True)
                await back_to_main(query, user_id)
            else:
                await query.answer("❌ لا تزال غير مشترك في بعض القنوات!", show_alert=True)
            
        elif query.data.startswith("buy_"):
            await handle_buy(query, context)
            
        elif query.data.startswith("join_channel_"):
            await handle_join_channel(query, user_id, context.bot)
            
        elif query.data.startswith("verify_channel_"):
            await handle_verify_channel(query, user_id, context.bot, context)
            
        elif query.data.startswith("report_channel_"):
            await handle_report_channel(query, user_id, context.bot)
            
        elif query.data == "admin_storage_info":
            if is_admin(query.from_user.id):
                await storage_info(query, context)
            else:
                await query.answer("❌ ليس لديك صلاحية الوصول!", show_alert=True)
                
        elif query.data == "refresh_storage_info":
            if is_admin(query.from_user.id):
                await storage_info(query, context)
            else:
                await query.answer("❌ ليس لديك صلاحية الوصول!", show_alert=True)
            
        elif query.data.startswith("admin_"):
            if not is_admin(query.from_user.id):
                await query.answer("❌ ليس لديك صلاحية الوصول!", show_alert=True)
                return
                
            action = query.data[6:]
            
            if action == "panel":
                await show_admin_panel(query)
            elif action == "stats":
                await show_admin_stats(query)
            elif action == "user_info":
                await query.edit_message_text(
                    "👤 معلومات مستخدم:\n\n"
                    "أرسل يوزر المستخدم (مثال: @username) أو ID:",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "user_info"
            elif action == "broadcast":
                await query.edit_message_text(
                    "📢 بث رسالة:\n\n"
                    "أرسل الرسالة التي تريد بثها للجميع:",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "broadcast"
            elif action == "give_points":
                await query.edit_message_text(
                    "💰 إضافة نقاط:\n\n"
                    "أرسل (اليوزر أو ID) و (عدد النقاط) مفصولين بمسافة:\n"
                    "مثال: @username 100\n"
                    "مثال: 12345678 100",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "give_points"
            elif action == "take_points":
                await query.edit_message_text(
                    "💸 خصم نقاط:\n\n"
                    "أرسل (اليوزر أو ID) و (عدد النقاط) مفصولين بمسافة:\n"
                    "مثال: @username 50\n"
                    "مثال: 12345678 50",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "take_points"
            elif action == "ban":
                await query.edit_message_text(
                    "⚠️ حظر مستخدم:\n\n"
                    "أرسل يوزر المستخدم (مثال: @username) أو ID:",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "ban_user"
            elif action == "unban":
                await query.edit_message_text(
                    "✅ فك حظر مستخدم:\n\n"
                    "أرسل يوزر المستخدم (مثال: @username) أو ID:",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "unban_user"
            elif action == "mute":
                await query.edit_message_text(
                    "🔇 كتم مستخدم:\n\n"
                    "أرسل (اليوزر أو ID) و (الوقت بالثواني) مفصولين بمسافة:\n"
                    "مثال: @username 3600 (ساعة)\n"
                    "مثال: 12345678 86400 (يوم)\n"
                    "مثال: @username 0 (كتم دائم)\n\n"
                    "للإبلاغ عن سبب الكتم، اكتب السبب بعد الوقت:\n"
                    "@username 3600 السبب هنا",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "mute_user"
            elif action == "unmute":
                await query.edit_message_text(
                    "🔊 فك كتم مستخدم:\n\n"
                    "أرسل يوزر المستخدم أو ID:\n"
                    "مثال: @username\n"
                    "مثال: 12345678",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "unmute_user"
            elif action == "add_channel":
                await query.edit_message_text(
                    "➕ إضافة قناة عادية:\n\n"
                    "أرسل (يوزر القناة) و (عدد الأعضاء) مفصولين بمسافة:\n"
                    "مثال: @channel_username 100",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "add_channel"
            elif action == "remove_channel":
                await query.edit_message_text(
                    "➖ حذف قناة عادية:\n\n"
                    "أرسل يوزر القناة:\n"
                    "مثال: @channel_username",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "remove_channel"
            elif action == "force_add":
                await query.edit_message_text(
                    "🔒 إضافة قناة اشتراك إجباري:\n\n"
                    "أرسل يوزر القناة:\n"
                    "مثال: @channel_username",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "add_force"
            elif action == "force_remove":
                await query.edit_message_text(
                    "🔓 حذف قناة اشتراك إجباري:\n\n"
                    "أرسل يوزر القناة:\n"
                    "مثال: @channel_username",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "remove_force"
            elif action == "add_code":
                await query.edit_message_text(
                    "🎟️ إضافة كود:\n\n"
                    "أرسل (اسم الكود) و (عدد النقاط) و (عدد المستخدمين) مفصولين بمسافة:\n"
                    "مثال: TUX100 100 10\n\n"
                    "• TUX100 = اسم الكود\n"
                    "• 100 = عدد النقاط\n"
                    "• 10 = عدد المستخدمين الذين يمكنهم استخدامه",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "add_code"
            elif action == "remove_code":
                await query.edit_message_text(
                    "🗑️ حذف كود:\n\n"
                    "أرسل اسم الكود:\n"
                    "مثال: TUX100",
                    parse_mode="HTML"
                )
                context.user_data["admin_action"] = "remove_code"
            elif action == "storage_info":
                await storage_info(query, context)
            
        else:
            await query.answer("❌ هذا الزر لا يعمل حالياً!", show_alert=True)
            
    except Exception as e:
        logger.error(f"خطأ في معالجة الزر: {e}")
        try:
            await query.answer(f"❌ حدث خطأ: {str(e)[:50]}", show_alert=True)
        except Exception:
            pass

# ===================== الهدية اليومية =====================

async def show_daily_gift(query, user_id):
    """عرض صفحة الهدية اليومية"""
    user_data = get_user_data(user_id)
    daily_gift = user_data.get("daily_gift", {})
    streak = daily_gift.get("streak", 0)
    total_claimed = daily_gift.get("total_claimed", 0)
    
    can_claim, time_remaining = await can_claim_daily_gift(user_id)
    
    text = "🎁 الهدية اليومية\n\n"
    
    if can_claim:
        text += "🎉 يمكنك المطالبة بالهدية اليومية الآن!\n\n"
        text += f"💰 الهدية: 3 نقاط\n"
        text += f"📊 السلسلة: {streak} يوم\n"
        text += f"🎯 المجموع: {total_claimed} مرة\n\n"
        text += "اضغط على الزر أدناه للحصول على 3 نقاط مجانية!"
        
        keyboard = [
            [InlineKeyboardButton("🎁 المطالبة بالهدية (3 نقاط)", callback_data="claim_daily_gift")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]
        ]
    else:
        text += "⏰ لقد حصلت على هديتك اليوم!\n\n"
        text += f"⏳ الوقت المتبقي: {time_remaining} ساعة\n"
        text += f"📊 السلسلة: {streak} يوم\n"
        text += f"🎯 المجموع: {total_claimed} مرة\n\n"
        text += f"🕐 آخر مطالبة: {daily_gift.get('last_claimed', 'لم تطالب من قبل')}"
        
        keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")

async def handle_claim_daily_gift(query, user_id, bot):
    """معالجة المطالبة بالهدية اليومية"""
    transaction_id = f"daily_{user_id}_{int(time.time() * 1000)}"
    
    # قفل للمستخدم
    lock_key = f"daily_{user_id}"
    _daily_locks.setdefault(lock_key, threading.Lock())
    
    with _daily_locks[lock_key]:
        user_data = get_user_data(user_id, force_reload=True)
        
        can_claim, time_remaining = await can_claim_daily_gift(user_id)
        
        if not can_claim:
            await query.answer(f"⏳ انتظر {time_remaining} للحصول على الهدية التالية!", show_alert=True)
            return
        
        # التحقق من Cooldown
        can_proceed, remaining, reason = cooldown_manager.can_proceed(
            user_id, "daily_gift", transaction_id
        )
        
        if not can_proceed:
            await query.answer(f"⏳ {reason}. انتظر {remaining:.1f} ثواني", show_alert=True)
            return
        
        # إضافة النقاط
        points_to_add = 3
        success, message = safe_add_points(
            user_id, 
            points_to_add, 
            "add", 
            "daily_gift",
            f"points_{transaction_id}"
        )
        
        if not success:
            await query.answer(f"❌ {message}", show_alert=True)
            return
        
        # تحديث بيانات الهدية اليومية
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        daily_gift = user_data.get("daily_gift", {})
        
        last_claimed = daily_gift.get("last_claimed")
        if last_claimed:
            try:
                last_date = datetime.strptime(last_claimed, "%Y-%m-%d %H:%M:%S")
                now_date = datetime.now()
                
                if (now_date - last_date).days <= 1:
                    streak = daily_gift.get("streak", 0) + 1
                else:
                    streak = 1
            except Exception:
                streak = 1
        else:
            streak = 1
        
        updates = {
            "daily_gift": {
                "last_claimed": now,
                "streak": streak,
                "total_claimed": daily_gift.get("total_claimed", 0) + 1
            }
        }
        
        if not update_user_data(user_id, updates, "daily_gift_update", transaction_id):
            await query.answer("❌ خطأ في تحديث البيانات!", show_alert=True)
            return
        
        # تحديث الإحصائيات مرة واحدة
        update_system_stats("total_daily_gifts", increment=1, points=points_to_add)
        
        # وضع علامة على المعاملة كمكتملة
        cooldown_manager.mark_transaction_complete(transaction_id)
        
        # إرسال رسالة تأكيد
        try:
            await bot.send_message(
                user_id,
                f"🎉 تم استلام هديتك اليومية!\n\n"
                f"💰 المكافأة: {points_to_add} نقاط\n"
                f"🎯 نقاطك الآن: {user_data['points'] + points_to_add}\n"
                f"📊 سلسلتك: {streak} يوم\n\n"
                f"⏰ الهدية التالية بعد 24 ساعة",
                parse_mode="HTML"
            )
        except Exception:
            pass
        
        # تحديث صفحة الهدية اليومية
        success_message = (
            f"✅ تم المطالبة بالهدية اليومية بنجاح!\n\n"
            f"💰 حصلت على: {points_to_add} نقاط\n"
            f"🎯 نقاطك الآن: {user_data['points'] + points_to_add}\n"
            f"📊 سلسلتك: {streak} يوم\n"
            f"🎁 المجموع: {daily_gift.get('total_claimed', 0) + 1} مرة\n\n"
            f"📩 تم إرسال رسالة تأكيد لك بالخصوص!"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(success_message, reply_markup=reply_markup, parse_mode="HTML")

# ===================== التجميع العادي =====================

def check_and_mark_completed_channels():
    """التحقق من القنوات المكتملة وحذفها من الملفات"""
    data = load_data(force_reload=True)
    users_data = load_users(force_reload=True)
    channels = data.get("channels", {})
    completed_count = 0
    deleted_channels = []
    
    for channel_id, channel_data in list(channels.items()):
        current = channel_data.get("current", 0)
        required = channel_data.get("required", 0)
        
        # تحقق من اكتمال القناة
        if current >= required and not channel_data.get("completed", False):
            channel_username = channel_data.get("username", "unknown")
            owner_id = channel_data.get("owner")
            
            logger.info(f"✅ القناة {channel_username} اكتملت - سيتم حذفها من الملفات")
            
            # تنظيف بيانات جميع المستخدمين المرتبطين بهذه القناة
            cleaned_users = 0
            for user_id, user_info in users_data.items():
                try:
                    needs_update = False
                    
                    # حذف من active_subscriptions
                    if "active_subscriptions" in user_info and channel_id in user_info["active_subscriptions"]:
                        user_info["active_subscriptions"] = [c for c in user_info["active_subscriptions"] if c != channel_id]
                        needs_update = True
                    
                    # حذف من joined_channels
                    if "joined_channels" in user_info and channel_id in user_info["joined_channels"]:
                        del user_info["joined_channels"][channel_id]
                        needs_update = True
                    
                    # حذف من temp_left_channels
                    if "temp_left_channels" in user_info and channel_id in user_info["temp_left_channels"]:
                        user_info["temp_left_channels"] = [c for c in user_info["temp_left_channels"] if c != channel_id]
                        needs_update = True
                    
                    # حذف من permanent_left_channels
                    if "permanent_left_channels" in user_info and channel_id in user_info["permanent_left_channels"]:
                        user_info["permanent_left_channels"] = [c for c in user_info["permanent_left_channels"] if c != channel_id]
                        needs_update = True
                    
                    # حذف من left_channels القديم
                    if "left_channels" in user_info and channel_id in user_info["left_channels"]:
                        user_info["left_channels"] = [c for c in user_info["left_channels"] if c != channel_id]
                        needs_update = True
                    
                    if needs_update:
                        users_data[user_id] = user_info
                        cleaned_users += 1
                        
                except Exception as e:
                    logger.error(f"خطأ في تنظيف بيانات المستخدم {user_id}: {e}")
            
            if cleaned_users > 0:
                save_users(users_data, backup=False)
                logger.info(f"🧹 تم تنظيف بيانات {cleaned_users} مستخدم للقناة {channel_username}")
            
            # حذف القناة من الملفات نهائياً
            deleted_channels.append({
                "id": channel_id,
                "username": channel_username,
                "owner": owner_id,
                "final_count": f"{current}/{required}",
                "deleted_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
            del channels[channel_id]
            completed_count += 1
    
    if completed_count > 0:
        data["channels"] = channels
        save_data(data, backup=False)
        logger.info(f"🎯 تم حذف {completed_count} قناة مكتملة من الملفات")
        
        # حفظ سجل القنوات المحذوفة (اختياري)
        if "deleted_channels_history" not in data:
            data["deleted_channels_history"] = []
        
        data["deleted_channels_history"].extend(deleted_channels)
        
        # الاحتفاظ بآخر 100 سجل فقط
        data["deleted_channels_history"] = data["deleted_channels_history"][-100:]
        save_data(data, backup=False)
    
    return completed_count

async def show_collect_points(query, user_id):
    """عرض قنوات التجميع - نسخة مُصلحة تسمح بالعودة"""
    check_and_mark_completed_channels()

    data = load_data(force_reload=True)
    user_data = get_user_data(user_id, force_reload=True)

    text = "📊 قنوات التجميع:\n\n"
    keyboard = []
    available_channels = 0
    hidden_channels = 0

    for channel_id, channel_data in data.get("channels", {}).items():
        channel_username = channel_data.get("username", "")
        if not channel_username:
            continue

        # تخطي القنوات المكتملة
        if channel_data.get("completed", False):
            continue

        # ✅ الإصلاح الرئيسي: استخدام منطق العرض الصحيح
        # بدلاً من can_user_join_channel (التي تمنع العرض)
        # نستخدم منطق مباشر يسمح بالعودة
        
        # 1. التحقق من صاحب القناة
        if str(user_id) == channel_data.get("owner"):
            hidden_channels += 1
            continue
        
        # 2. التحقق من الأدمن
        if channel_data.get("owner") == str(ADMIN_ID) and is_admin(int(user_id)):
            hidden_channels += 1
            continue
        
        # 3. فحص حالة الانضمام
        joined_channels = user_data.get("joined_channels", {})
        current_round = channel_data.get("reuse_count", 0)
        
        should_show = True  # افتراضياً نعرض القناة
        
        if channel_id in joined_channels:
            join_info = joined_channels[channel_id]
            user_round = join_info.get("round", 0)
            
            # أ. منضم حالياً ولم يغادر في نفس الجولة
            if (join_info.get("verified", False) and 
                not join_info.get("left", False) and 
                user_round == current_round):
                should_show = False
                
            # ب. غادر قناة مكتملة في نفس الجولة
            elif (join_info.get("left_completed", False) and 
                  join_info.get("completed_round", 0) == current_round):
                should_show = False
        
        # 4. التحقق الفعلي من تيليجرام (للتأكد)
        if should_show:
            try:
                real_sub = await check_channel_subscription(
                    query.get_bot(),
                    int(user_id),
                    channel_username
                )

                # إذا كان مشتركاً فعلياً ولم يسجل انضمامه
                if real_sub is True:
                    if channel_id not in joined_channels or not joined_channels[channel_id].get("verified", False):
                        # تسجيل تلقائي
                        joined_channels[channel_id] = {
                            "verified": True,
                            "left": False,
                            "round": current_round,
                            "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        
                        update_user_data(
                            user_id,
                            {"joined_channels": joined_channels},
                            "auto_verify_existing_join"
                        )
                        
                        should_show = False  # مشترك → لا نعرض
                    elif not joined_channels[channel_id].get("left", False):
                        should_show = False  # مشترك ولم يغادر → لا نعرض
                        
            except Exception as e:
                logger.error(f"خطأ في التحقق من الاشتراك: {e}")
        
        if not should_show:
            hidden_channels += 1
            continue

        # ✅ القناة تُعرض للمستخدم
        available_channels += 1

        progress = f"{channel_data.get('current', 0)}/{channel_data.get('required', 0)}"
        current_round_display = channel_data.get("reuse_count", 0)

        if current_round_display > 0:
            text += f"📢 @{channel_username} - {progress} (الجولة {current_round_display + 1})\n"
        else:
            text += f"📢 @{channel_username} - {progress}\n"

        channel_link = f"https://t.me/{channel_username.replace('@', '')}"
        can_report = can_user_report_channel(user_id, channel_id)

        keyboard.append([
            InlineKeyboardButton(f"📲 @{channel_username}", url=channel_link),
            InlineKeyboardButton("✅ انضم (3 نقاط)", callback_data=f"join_channel_{channel_id}"),
            InlineKeyboardButton(
                "🚨 إبلاغ",
                callback_data=f"report_channel_{channel_id}"
            ) if can_report else InlineKeyboardButton("✅ تم الإبلاغ", callback_data="report_disabled")
        ])

    if available_channels == 0:
        text = (
            "📭 لا توجد قنوات متاحة لك حالياً.\n\n"
            "💡 أسباب الإخفاء:\n"
            "• أنت مشترك بها مسبقاً\n"
            "• القناة مكتملة\n"
            "• أنت صاحب القناة\n"
            "• انتظر قنوات جديدة"
        )

    keyboard.append([InlineKeyboardButton("🔄 تحديث القائمة", callback_data="collect_points")])
    keyboard.append([InlineKeyboardButton("🔙 رجوع", callback_data="back_main")])

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode="HTML"
    )

async def handle_join_channel(query, user_id, bot):
    """معالجة الانضمام للقناة"""
    await query.answer()
    
    channel_id = query.data.replace("join_channel_", "")
    
    data = load_data()
    
    if channel_id not in data.get("channels", {}):
        await query.answer("❌ القناة غير متاحة", show_alert=True)
        return
    
    channel = data["channels"][channel_id]
    channel_username = channel.get("username", "")
    
    if not channel_username:
        await query.answer("❌ القناة غير صالحة", show_alert=True)
        return
    
    can_join, reason = can_user_join_channel(user_id, channel_id, channel_username)
    if not can_join:
        await query.answer(reason, show_alert=True)
        return
    
    channel_link = f"https://t.me/{channel_username.replace('@', '')}"
    
    try:
        await query.edit_message_text(
            f"📢 @{channel_username}\n\n"
            f"1️⃣ اشترك في القناة من الرابط أدناه\n"
            f"2️⃣ انتظر 5-10 ثواني\n"
            f"3️⃣ اضغط زر التحقق بالأسفل\n\n"
            f"💰 المكافأة: 3 نقاط",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📲 رابط القناة", url=channel_link)],
                [InlineKeyboardButton("✅ تحقق والحصول على 3 نقاط", callback_data=f"verify_channel_{channel_id}")]
            ])
        )
    except Exception:
        await query.message.reply_text(
            f"📢 @{channel_username}\n\n"
            f"1️⃣ اشترك في القناة من الرابط أدناه\n"
            f"2️⃣ انتظر 5-10 ثواني\n"
            f"3️⃣ اضغط زر التحقق\n\n"
            f"💰 المكافأة: 3 نقاط",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📲 رابط القناة", url=channel_link)],
                [InlineKeyboardButton("✅ تحقق والحصول على 3 نقاط", callback_data=f"verify_channel_{channel_id}")]
            ])
        )

async def handle_verify_channel(query, user_id, bot, context):
    """معالجة التحقق من الانضمام للقناة (نسخة محسنة بنظام الدورات) - مُصلحة"""
    try:
        await query.answer("⏳ جاري التحقق من اشتراكك...")
    except Exception:
        pass
    
    channel_id = query.data.replace("verify_channel_", "")
    transaction_id = f"verify_{user_id}_{channel_id}_{int(time.time() * 1000)}"
    
    # التحقق من Cooldown والمعاملات المكررة
    can_proceed, remaining, reason = cooldown_manager.can_proceed(
        user_id, "verify_channel", transaction_id
    )
    
    if not can_proceed:
        await query.answer(f"⏳ {reason}. انتظر {remaining:.1f} ثواني", show_alert=True)
        return
    
    # قفل للقناة والمستخدم معاً
    lock_key = f"verify_{channel_id}_{user_id}"
    _verify_locks.setdefault(lock_key, threading.Lock())
    
    with _verify_locks[lock_key]:
        data = load_data(force_reload=True)
        
        # التحقق من وجود القناة
        if channel_id not in data.get("channels", {}):
            await query.answer("❌ القناة غير متاحة", show_alert=True)
            cooldown_manager.mark_transaction_complete(transaction_id)
            return
        
        channel = data["channels"][channel_id]
        channel_username = channel.get("username", "")
        
        if not channel_username:
            await query.answer("❌ القناة غير صالحة", show_alert=True)
            cooldown_manager.mark_transaction_complete(transaction_id)
            return
        
        # التحقق من اكتمال القناة
        if channel.get("completed", False):
            await query.answer("❌ هذه القناة اكتملت بالفعل!", show_alert=True)
            cooldown_manager.mark_transaction_complete(transaction_id)
            return
        
        # ======== التحققات المُصلحة ========
        # منع صاحب القناة من الانضمام
        if str(user_id) == channel.get("owner"):
            await query.answer("❌ لا يمكنك الانضمام لقناتك الخاصة!", show_alert=True)
            cooldown_manager.mark_transaction_complete(transaction_id)
            return
        
        # منع الأدمن من الانضمام للقنوات الإدارية
        if channel.get("owner") == str(ADMIN_ID) and is_admin(int(user_id)):
            await query.answer("❌ لا يمكنك الانضمام لقناة الإدارة!", show_alert=True)
            cooldown_manager.mark_transaction_complete(transaction_id)
            return
        
        # الحصول على معلومات إعادة التفعيل
        reactivated_at = channel.get("reactivated_at")
        current_round = channel.get("reuse_count", 0)
        
        # التحقق الشامل من حالة المستخدم مع مراعاة إعادة التفعيل
        user_data = get_user_data(user_id, force_reload=True)
        joined_channels = user_data.get("joined_channels", {})
        
        if channel_id in joined_channels:
            join_info = joined_channels[channel_id]
            
            # 🔴 🔴 🔴 التحقق من left_completed أولاً - الإصلاح الرئيسي 🔴 🔴 🔴
            if join_info.get("left_completed", False):
                completed_round = join_info.get("completed_round", 0)
                
                # ✅ إذا كانت جولة جديدة → يمكنه الانضمام (لا نرجع، نكمل)
                if current_round > completed_round:
                    logger.info(
                        f"✅ المستخدم {user_id} يعود للقناة {channel_id} "
                        f"(غادر جولة {completed_round}، الآن جولة {current_round})"
                    )
                    # نسمح له بالمتابعة - لا return هنا!
                    pass
                else:
                    # ❌ نفس الجولة المكتملة - هنا فقط نمنعه
                    await query.answer("❌ لا يمكنك الانضمام لنفس الجولة المكتملة!", show_alert=True)
                    cooldown_manager.mark_transaction_complete(transaction_id)
                    return
            
            # التحقق من إعادة التفعيل (للحالات الأخرى)
            elif reactivated_at and "joined_at" in join_info:
                try:
                    join_time = datetime.strptime(join_info["joined_at"], "%Y-%m-%d %H:%M:%S")
                    reactivate_time = datetime.strptime(reactivated_at, "%Y-%m-%d %H:%M:%S")
                    
                    # إذا انضم قبل إعادة التفعيل ولا يزال منضماً (بدون left)
                    if join_time < reactivate_time and join_info.get("verified", False) and not join_info.get("left", False):
                        await query.answer("❌ انضممت للنسخة القديمة من هذه القناة!", show_alert=True)
                        cooldown_manager.mark_transaction_complete(transaction_id)
                        return
                    
                    # إذا انضم قبل إعادة التفعيل وغادر
                    elif join_time < reactivate_time and join_info.get("left", False):
                        # هذا جيد - يمكنه الانضمام للنسخة الجديدة
                        pass
                    
                    # إذا انضم بعد إعادة التفعيل
                    else:
                        if join_info.get("verified", False) and not join_info.get("left", False):
                            await query.answer("❌ سبق أن انضممت لهذه القناة في الجولة الحالية!", show_alert=True)
                            cooldown_manager.mark_transaction_complete(transaction_id)
                            return
                        
                except Exception as e:
                    logger.error(f"خطأ في مقارنة التواريخ: {e}")
            
            # بدون إعادة تفعيل ولا left_completed
            elif join_info.get("verified", False) and not join_info.get("left", False):
                await query.answer("❌ سبق أن انضممت لهذه القناة!", show_alert=True)
                cooldown_manager.mark_transaction_complete(transaction_id)
                return
        # ======== نهاية التحققات المُصلحة ========
        
        # التحقق من اشتراك المستخدم في القناة
        try:
            is_subscribed = await check_channel_subscription(bot, int(user_id), channel_username)
            
            if is_subscribed is None:
                await query.edit_message_text(
                    f"⚠️ حدث خطأ في التحقق\n\n"
                    f"📢 @{channel_username}\n\n"
                    f"🔧 البوت لا يستطيع التحقق من الاشتراك.",
                    parse_mode="HTML"
                )
                cooldown_manager.mark_transaction_complete(transaction_id)
                return
            
            if not is_subscribed:
                await query.edit_message_text(
                    f"❌ أنت غير مشترك بالقناة!\n\n"
                    f"📢 @{channel_username}\n\n"
                    f"🔗 اشترك في القناة أولاً ثم اضغط تحقق مرة أخرى",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("📲 اشترك في القناة", url=f"https://t.me/{channel_username.replace('@', '')}")],
                        [InlineKeyboardButton("🔄 تحقق مرة أخرى", callback_data=f"verify_channel_{channel_id}")]
                    ])
                )
                cooldown_manager.mark_transaction_complete(transaction_id)
                return
                
        except Exception as e:
            logger.error(f"خطأ في التحقق من الاشتراك: {e}")
            await query.edit_message_text(
                f"⚠️ حدث خطأ في التحقق\n\n"
                f"📢 @{channel_username}\n\n"
                f"🔧 حاول مرة أخرى بعد قليل.",
                parse_mode="HTML"
            )
            cooldown_manager.mark_transaction_complete(transaction_id)
            return
        
        # ✅ المستخدم مشترك - منح النقاط
        points_to_add = 3
        transaction_id_points = f"points_{user_id}_{channel_id}_{int(time.time() * 1000)}"
        
        success, message = safe_add_points(
            user_id, 
            points_to_add, 
            "add", 
            "channel_join",
            transaction_id_points
        )
        
        if not success:
            await query.answer(f"❌ {message}", show_alert=True)
            cooldown_manager.mark_transaction_complete(transaction_id)
            return
        
        # تحديث معلومات الانضمام باستخدام الدالة الجديدة
        update_success, join_info = update_user_channel_join_info(
            user_id=user_id,
            channel_id=channel_id,
            channel_username=channel_username,
            current_round=current_round,
            reactivated_at=reactivated_at,
            points_earned=points_to_add,
            transaction_id=transaction_id
        )
        
        if not update_success:
            await query.answer("❌ حدث خطأ في تحديث بيانات الانضمام!", show_alert=True)
            cooldown_manager.mark_transaction_complete(transaction_id)
            return
        
        # تحديث بيانات القناة
        current_count = channel.get("current", 0) + 1
        required_count = channel.get("required", 0)
        
        channel["current"] = current_count
        channel["last_activity"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # ✅ التحقق إذا كان المستخدم عائد (غادر سابقاً)
        user_data_check = get_user_data(user_id, force_reload=True)
        joined_channels_check = user_data_check.get("joined_channels", {})
        
        is_returning_user = False
        previous_leave_time = None
        
        if channel_id in joined_channels_check:
            join_info_check = joined_channels_check[channel_id]
            if join_info_check.get("left", False):
                is_returning_user = True
                previous_leave_time = join_info_check.get("left_at")
                logger.info(
                    f"🔄 المستخدم {user_id} يعود للقناة {channel_id} "
                    f"(غادر في: {previous_leave_time})"
                )
        
        # ✅ تسجيل في سجل العودة إذا كان عائداً
        if is_returning_user:
            if "return_history" not in channel:
                channel["return_history"] = []
            
            channel["return_history"].append({
                "user_id": user_id,
                "returned_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "previous_leave": previous_leave_time,
                "previous_count": current_count - 1,
                "new_count": current_count,
                "points_earned": 3
            })
        
        if "joined_users" not in channel:
            channel["joined_users"] = []
        
        # إضافة المستخدم مع معلومات الجولة
        channel["joined_users"].append({
            "user_id": user_id,
            "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "round": current_round,
            "reactivated_at": reactivated_at,
            "returning": is_returning_user
        })
        
        # التحقق من اكتمال القناة
        if current_count >= required_count:
            channel["completed"] = True
            channel["completed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"✅ تم إكمال القناة {channel_username} - {current_count}/{required_count}")
            
            # 🔴 🔴 🔴 إرسال إشعارات فورية عند الاكتمال 🔴 🔴 🔴
            try:
                await send_channel_completion_notifications(
                    bot=bot,
                    channel_username=channel_username,
                    owner_id=channel.get("owner"),
                    current_count=current_count,
                    required_count=required_count,
                    created_at=channel.get("created_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                )
            except Exception as e:
                logger.error(f"❌ خطأ في إرسال إشعارات الاكتمال: {e}")
        
        # تحديث بيانات القناة
        data["channels"][channel_id] = channel
        
        if not save_data(data, backup=False):
            logger.error(f"خطأ في حفظ بيانات القناة {channel_id}")
        
        # وضع علامة على المعاملة كمكتملة
        cooldown_manager.mark_transaction_complete(transaction_id)
        
        # ======== إرسال إشعار انضمام للمالك ========
        try:
            await send_join_notification_to_owner(
                bot=bot,
                channel_username=channel_username,
                owner_id=channel.get("owner"),
                user_data=user_data,
                is_returning_user=is_returning_user,
                current_count=current_count,
                required_count=required_count,
                current_round=current_round
            )
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال إشعار الانضمام: {e}")
        
        # الحصول على بيانات المستخدم المحدثة
        updated_user_data = get_user_data(user_id, force_reload=True)
        
        # إعداد رسالة النجاح
        success_message = (
            f"✅ تم التحقق بنجاح!\n\n"
            f"📢 القناة: @{channel_username}\n"
            f"💰 حصلت على: {points_to_add} نقاط\n"
            f"🎯 نقاطك الآن: {updated_user_data['points']}\n"
            f"📊 العداد: {current_count - 1} → {current_count}/{required_count}"
        )
        
        # إضافة رسالة العودة
        if is_returning_user:
            success_message += (
                f"\n\n🔄 مرحباً بعودتك!\n"
                f"✅ تم زيادة العداد من جديد\n"
                f"💡 احرص على البقاء في القناة"
            )
        
        # إضافة معلومات الجولة إذا كانت هناك إعادة تفعيل
        if current_round > 0:
            success_message += f"\n🔄 الجولة: {current_round + 1}"
        
        if current_count >= required_count:
            success_message += f"\n\n🎉 القناة اكتملت!"
        
        success_message += f"\n\n🎉 استمر في جمع النقاط من القنوات الأخرى!"
        
        try:
            await query.edit_message_text(
                success_message,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📊 المزيد من القنوات", callback_data="collect_points")],
                    [InlineKeyboardButton("🏠 القائمة الرئيسية", callback_data="back_main")]
                ])
            )
            
        except Exception as e:
            logger.error(f"خطأ في تحديث رسالة الاستعلام: {e}")
            try:
                await query.message.reply_text(
                    success_message,
                    parse_mode="HTML"
                )
            except Exception:
                pass


# ===================== المتجر والشراء =====================

async def show_store(query):
    """عرض المتجر"""
    keyboard = [
        [InlineKeyboardButton("10 أعضاء - 20 نقطة", callback_data="buy_10")],
        [InlineKeyboardButton("25 عضو - 50 نقطة", callback_data="buy_25")],
        [InlineKeyboardButton("50 عضو - 100 نقطة", callback_data="buy_50")],
        [InlineKeyboardButton("100 أعضاء - 200 نقطة", callback_data="buy_100")],
        [InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("🛒 المتجر:\nاختر العرض المناسب:", reply_markup=reply_markup, parse_mode="HTML")

async def handle_buy(query, context: ContextTypes.DEFAULT_TYPE):
    """معالجة عملية الشراء"""
    user_id = str(query.from_user.id)
    user_data = get_user_data(user_id)
    
    num_members = int(query.data.split("_")[1])
    points_needed = num_members * 2
    
    if user_data["points"] < points_needed:
        keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"❌ نقاطك غير كافية! تحتاج {points_needed} نقطة.", reply_markup=reply_markup)
        return
    
    transaction_id = f"buy_{user_id}_{int(time.time() * 1000)}"
    
    # التحقق من Cooldown
    can_proceed, remaining, reason = cooldown_manager.can_proceed(
        user_id, "store", transaction_id
    )
    
    if not can_proceed:
        await query.answer(f"⏳ {reason}. انتظر {remaining:.1f} ثواني", show_alert=True)
        return
    
    context.user_data["buying"] = {
        "members": num_members,
        "points": points_needed,
        "user_id": user_id,
        "transaction_id": transaction_id
    }
    
    await query.edit_message_text(
        f"🛒 شراء {num_members} عضو\n💰 السعر: {points_needed} نقطة\n\n"
        "أرسل يوزر القناة (مثال: @channel_username):",
        parse_mode="HTML"
    )

async def show_invite_link(query, user_id, bot):
    """عرض رابط الدعوة"""
    bot_username = (await bot.get_me()).username
    invite_link = f"https://t.me/{bot_username}?start={user_id}"
    
    user_data = get_user_data(user_id)
    
    keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"🔗 رابط دعوتك الخاص:\n\n"
        f"{invite_link}\n\n"
        f"📊 أحصائيات الدعوة:\n"
        f"👥 عدد المدعوين: {user_data['invites']}\n"
        f"💰 النقاط من الدعوة: {user_data['invites'] * 4}\n\n"
        f"🎯 كل صديق يدخل عبر رابطك يحصل على 4 نقاط!\n"
        f"📢 شارك الرابط مع أصدقائك واحصل على نقاط مجانية.\n\n"
        f"📢 قناة البوت الرسمية: {BOT_CHANNEL}",
        reply_markup=reply_markup,
        parse_mode="HTML"
    )

def cleanup_permanent_left_channels(context: ContextTypes.DEFAULT_TYPE = None):
    """تنظيف القنوات المتروكة نهائياً (تغيير النظام)"""
    try:
        users_data = load_users()
        cleaned_count = 0
        
        for user_id, user_data in users_data.items():
            if "permanent_left_channels" in user_data and user_data["permanent_left_channels"]:
                # ننقل جميع القنوات من permanent_left إلى temp_left
                temp_left = user_data.get("temp_left_channels", [])
                permanent_left = user_data["permanent_left_channels"]
                
                for channel_id in permanent_left:
                    if channel_id not in temp_left:
                        temp_left.append(channel_id)
                
                updates = {
                    "temp_left_channels": temp_left,
                    "permanent_left_channels": []  # تفريغ القائمة
                }
                
                if update_user_data(user_id, updates, "cleanup_permanent_left"):
                    cleaned_count += len(permanent_left)
        
        if cleaned_count > 0:
            logger.info(f"🧹 تم تنظيف {cleaned_count} قناة من permanent_left_channels")
        
        return cleaned_count
    except Exception as e:
        logger.error(f"خطأ في تنظيف permanent_left_channels: {e}")
        return 0
        
async def show_top(query):
    """عرض التوب"""
    users_data = load_users()
    data = load_data()
    
    users_points = []
    for uid, user_data in users_data.items():
        if uid not in data.get("admins", []):
            users_points.append((uid, user_data.get("points", 0), user_data.get("username", "بدون يوزر")))
    
    users_points.sort(key=lambda x: x[1], reverse=True)
    
    text = "🏆 توب النقاط:\n\n"
    for i, (uid, points, username) in enumerate(users_points[:10], 1):
        status = ""
        if is_banned(int(uid)):
            status = "🚫 "
        elif is_muted(uid)[0]:
            status = "🔇 "
        
        text += f"{i}. {status}@{username}: {points} نقطة\n"
    
    text += "\n🏆 توب الدعوات:\n\n"
    
    users_invites = []
    for uid, user_data in users_data.items():
        if uid not in data.get("admins", []):
            users_invites.append((uid, user_data.get("invites", 0), user_data.get("username", "بدون يوزر")))
    
    users_invites.sort(key=lambda x: x[1], reverse=True)
    
    for i, (uid, invites, username) in enumerate(users_invites[:10], 1):
        status = ""
        if is_banned(int(uid)):
            status = "🚫 "
        elif is_muted(uid)[0]:
            status = "🔇 "
        
        text += f"{i}. {status}@{username}: {invites} دعوة\n"
    
    keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")

async def show_codes_panel(query):
    text = (
        "🎟️ نظام الأكواد\n\n"
        "🔐 الأكواد غير معروضة حفاظًا على الخصوصية\n\n"
        "📝 لاستخدام كود:\n"
        "أرسل:\n"
        "/code كود_هنا\n\n"
        "📌 مثال:\n"
        "/code TUX100"
    )

    keyboard = [[InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await query.edit_message_text(
        text,
        reply_markup=reply_markup,
        parse_mode="HTML"
    )

async def handle_report_channel(query, user_id, bot):
    """معالجة الإبلاغ عن قناة"""
    channel_id = query.data.replace("report_channel_", "")
    channel_type = "عادية"
    
    if not can_user_report_channel(user_id, channel_id):
        await query.answer(f"⚠️ لقد أبلغت عن هذه القناة {channel_type} مسبقاً!", show_alert=True)
        return
    
    data = load_data()
    if channel_id in data.get("channels", {}):
        channel = data["channels"][channel_id]
        
        if "reports" not in data:
            data["reports"] = {}
        
        report_id = f"report_{int(time.time())}"
        data["reports"][report_id] = {
            "channel_id": channel_id,
            "channel_username": channel.get("username", ""),
            "channel_type": channel_type,
            "reporter_id": user_id,
            "reporter_username": get_user_data(user_id).get("username", ""),
            "reason": "عدم الاشتراك أو مشكلة في القناة",
            "status": "pending",
            "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        save_data(data)
        
        add_user_reported_channel(user_id, channel_id)
        
        await query.answer(f"✅ تم تسجيل إبلاغك عن القناة {channel_type}، سيقوم الأدمن بمراجعته.", show_alert=True)
        
        admin_msg = (
            f"🚨 إبلاغ جديد عن قناة {channel_type}!\n\n"
            f"📢 القناة: @{channel['username']}\n"
            f"📋 النوع: {channel_type}\n"
            f"👤 المبلغ: @{get_user_data(user_id).get('username', 'بدون يوزر')}\n"
            f"🆔 ID المبلغ: {user_id}\n"
            f"📅 الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            f"🔍 معرف القناة: {channel_id}"
        )
        await send_to_admin(bot, admin_msg)
    else:
        await query.answer(f"❌ القناة {channel_type} غير موجودة!", show_alert=True)

async def back_to_main(query, user_id):
    """العودة للقائمة الرئيسية"""
    user_data = get_user_data(user_id)
    
    can_claim, time_remaining = await can_claim_daily_gift(user_id)
    daily_status = "🟢" if can_claim else "🔴"
    
    keyboard = [
        [InlineKeyboardButton("🛒 المتجر", callback_data="store")],
        [InlineKeyboardButton("📊 جمع النقاط", callback_data="collect_points")],
        [InlineKeyboardButton(f"{daily_status} الهدية اليومية", callback_data="daily_gift")],
        [InlineKeyboardButton("🏆 التوب", callback_data="top")],
        [InlineKeyboardButton("🔗 رابط الدعوة", callback_data="invite_link")],
        [InlineKeyboardButton("🎟️ الأكواد", callback_data="codes")],
    ]
    
    if is_admin(query.from_user.id):
        keyboard.append([InlineKeyboardButton("👑 لوحة الإدمن", callback_data="admin_panel")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    daily_info = ""
    if not can_claim:
        daily_info = f"⏳ الهدية متاحة بعد: {time_remaining}\n"
    
    await query.edit_message_text(
        f"👋 أهلاً {query.from_user.first_name}!\n"
        f"🎯 نقاطك: {user_data['points']}\n"
        f"🔗 عدد الدعوات: {user_data['invites']}\n"
        f"{daily_info}\n"
        f"📢 قناة البوت: {BOT_CHANNEL}\n\n"
        "اختر من القائمة:",
        reply_markup=reply_markup,
        parse_mode="HTML"
    )

# ===================== لوحة الإدمن =====================

async def show_admin_panel(query):
    """عرض لوحة الإدمن مع زر التخزين"""
    if not is_admin(query.from_user.id):
        await query.answer("❌ ليس لديك صلاحية الوصول!", show_alert=True)
        return
    
    keyboard = [
        [InlineKeyboardButton("⚠️ حظر مستخدم", callback_data="admin_ban"),
         InlineKeyboardButton("✅ فك حظر", callback_data="admin_unban")],
        [InlineKeyboardButton("🔇 كتم مستخدم", callback_data="admin_mute"),
         InlineKeyboardButton("🔊 فك كتم", callback_data="admin_unmute")],
        [InlineKeyboardButton("➕ إضافة قناة", callback_data="admin_add_channel"),
         InlineKeyboardButton("➖ حذف قناة", callback_data="admin_remove_channel")],
        [InlineKeyboardButton("🔒 قناة اشتراك إجباري", callback_data="admin_force_add"),
         InlineKeyboardButton("🔓 حذف قناة إجباري", callback_data="admin_force_remove")],
        [InlineKeyboardButton("🎟️ إضافة كود", callback_data="admin_add_code"),
         InlineKeyboardButton("🗑️ حذف كود", callback_data="admin_remove_code")],
        [InlineKeyboardButton("💰 إضافة نقاط", callback_data="admin_give_points"),
         InlineKeyboardButton("💸 خصم نقاط", callback_data="admin_take_points")],
        [InlineKeyboardButton("👤 معلومات مستخدم", callback_data="admin_user_info"),
         InlineKeyboardButton("📊 إحصائيات", callback_data="admin_stats")],
        [InlineKeyboardButton("📢 بث رسالة", callback_data="admin_broadcast"),
         InlineKeyboardButton("💾 حالة التخزين", callback_data="admin_storage_info")],  # أضف هذا السطر
        [InlineKeyboardButton("🔙 رجوع", callback_data="back_main")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        "👑 لوحة الإدمن\n\n"
        "اختر الإجراء المطلوب:",
        reply_markup=reply_markup,
        parse_mode="HTML"
    )

async def show_admin_stats(query):
    """عرض إحصائيات البوت"""
    data = load_data()
    users_data = load_users()
    stats = data.get("stats", {})
    
    active_users = 0
    week_ago = datetime.now().timestamp() - (7 * 24 * 60 * 60)
    
    for uid, user_data in users_data.items():
        last_active_str = user_data.get("last_active", "")
        if last_active_str:
            try:
                last_active = datetime.strptime(last_active_str, "%Y-%m-%d %H:%M:%S").timestamp()
                if last_active > week_ago:
                    active_users += 1
            except:
                pass
    
    completed_channels = 0
    active_channels = 0
    for channel_id, channel_data in data.get("channels", {}).items():
        if channel_data.get("completed", False):
            completed_channels += 1
        else:
            active_channels += 1
    
    total_daily_gifts = 0
    for uid, user_data in users_data.items():
        daily_gift = user_data.get("daily_gift", {})
        total_daily_gifts += daily_gift.get("total_claimed", 0)
    
    text = (
        f"📊 إحصائيات البوت الكاملة:\n\n"
        
        f"👥 المستخدمين:\n"
        f"• إجمالي المستخدمين: {stats.get('total_users', 0)}\n"
        f"• المستخدمين النشطين: {active_users}\n"
        f"• عدد المحظورين: {len(data.get('banned_users', []))}\n"
        f"• عدد المكتومين: {len(data.get('muted_users', {}))}\n"
        f"• عدد الأدمن: {len(data.get('admins', []))}\n\n"
        
        f"💰 النقاط:\n"
        f"• إجمالي النقاط: {stats.get('total_points', 0)}\n"
        f"• إجمالي الدعوات: {stats.get('total_invites', 0)}\n"
        f"• إجمالي المشتريات: {stats.get('total_purchases', 0)}\n"
        f"• إجمالي الانضمامات: {stats.get('total_joins', 0)}\n"
        f"• إجمالي الهدايا اليومية: {total_daily_gifts}\n\n"
        
        f"📢 القنوات:\n"
        f"• إجمالي القنوات: {len(data.get('channels', {}))}\n"
        f"• القنوات النشطة: {active_channels}\n"
        f"• القنوات المكتملة: {completed_channels}\n"
        f"• قنوات الإجباري: {len(data.get('force_sub_channels', []))}\n"
        f"• الأكواد النشطة: {len(data.get('codes', {}))}\n"
        f"• البلاغات النشطة: {len(data.get('reports', {}))}\n\n"
        
        f"📅 آخر تحديث:\n"
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    
    keyboard = [[InlineKeyboardButton("🔙 رجوع للوحة", callback_data="admin_panel")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode="HTML")

def get_user_statistics():
    """الحصول على إحصائيات شاملة للمستخدمين"""
    try:
        users_data = load_users()
        data = load_data()
        
        stats = {
            "total_users": len(users_data),
            "active_users": 0,
            "new_today": 0,
            "new_week": 0,
            "new_month": 0,
            "with_username": 0,
            "with_invites": 0,
            "banned_users": len(data.get("banned_users", [])),
            "muted_users": len(data.get("muted_users", {})),
            "total_points": 0,
            "total_invites": 0
        }
        
        today = datetime.now().date()
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)
        
        for uid, user_data in users_data.items():
            # النقاط والدعوات
            stats["total_points"] += user_data.get("points", 0)
            stats["total_invites"] += user_data.get("invites", 0)
            
            # اليوزرنيم
            if user_data.get("username"):
                stats["with_username"] += 1
            
            # الدعوات
            if user_data.get("invites", 0) > 0:
                stats["with_invites"] += 1
            
            # النشاط
            last_active_str = user_data.get("last_active", "")
            if last_active_str:
                try:
                    last_active_date = datetime.strptime(last_active_str, "%Y-%m-%d %H:%M:%S").date()
                    if last_active_date == today:
                        stats["active_users"] += 1
                except:
                    pass
            
            # التواريخ
            first_join_str = user_data.get("first_join", "")
            if first_join_str:
                try:
                    join_date = datetime.strptime(first_join_str, "%Y-%m-%d %H:%M:%S").date()
                    
                    if join_date == today:
                        stats["new_today"] += 1
                    elif join_date >= week_ago:
                        stats["new_week"] += 1
                    elif join_date >= month_ago:
                        stats["new_month"] += 1
                        
                except:
                    pass
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ خطأ في get_user_statistics: {e}")
        return None

async def check_and_remove_channel_if_bot_not_admin(bot, context: ContextTypes.DEFAULT_TYPE = None):
    """فحص جميع القنوات وإزالة القنوات التي لم يعد البوت مشرفاً فيها"""
    try:
        data = load_data(force_reload=True)
        channels = data.get("channels", {})
        removed_channels = []
        
        for channel_id, channel_data in list(channels.items()):
            channel_username = channel_data.get("username", "")
            
            if not channel_username:
                continue
            
            # تخطي القنوات المكتملة
            if channel_data.get("completed", False):
                continue
            
            try:
                bot_is_admin = await check_bot_is_admin(bot, channel_username)
                
                if not bot_is_admin:
                    # البوت لم يعد مشرفاً - حذف القناة
                    channel_info = {
                        "id": channel_id,
                        "username": channel_username,
                        "owner": channel_data.get("owner", "unknown"),
                        "required": channel_data.get("required", 0),
                        "current": channel_data.get("current", 0),
                        "created_at": channel_data.get("created_at", "unknown"),
                        "removed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "reason": "البوت لم يعد مشرفاً في القناة"
                    }
                    
                    # إضافة للسجل
                    if "removed_channels_history" not in data:
                        data["removed_channels_history"] = []
                    data["removed_channels_history"].append(channel_info)
                    
                    # الحصول على معلومات المالك
                    owner_id = channel_data.get("owner")
                    owner_name = "غير معروف"
                    if owner_id and owner_id != str(ADMIN_ID):
                        owner_data = get_user_data(owner_id)
                        owner_name = f"@{owner_data.get('username', owner_id)}"
                    
                    # إرسال إشعار للمالك
                    if owner_id and owner_id != str(ADMIN_ID):
                        try:
                            await bot.send_message(
                                int(owner_id),
                                f"⚠️ تم إلغاء طلب القناة!\n\n"
                                f"📢 القناة: @{channel_username}\n"
                                f"📊 التقدم: {channel_data.get('current', 0)}/{channel_data.get('required', 0)}\n"
                                f"💡 السبب: البوت لم يعد مشرفاً في القناة\n\n"
                                f"🔧 لإعادة التفعيل:\n"
                                f"1. أضف البوت كمشرف في القناة\n"
                                f"2. اشترِ أعضاء جديدة للقناة\n\n"
                                f"💰 تمت إعادة نقاط الطلب لحسابك",
                                parse_mode="HTML"
                            )
                        except Exception as e:
                            logger.error(f"❌ خطأ في إرسال إشعار للمالك {owner_id}: {e}")
                    
                    # إعادة النقاط للمالك
                    if owner_id and owner_id != str(ADMIN_ID):
                        # حساب النقاط التي تم دفعها
                        required_members = channel_data.get("required", 0)
                        points_paid = required_members * 2
                        
                        if points_paid > 0:
                            # إعادة النقاط
                            transaction_id = f"refund_{channel_id}_{int(time.time() * 1000)}"
                            success, message = safe_add_points(
                                owner_id, 
                                points_paid, 
                                "add", 
                                "bot_not_admin_refund",
                                transaction_id
                            )
                            
                            if success:
                                logger.info(f"✅ تم إعادة {points_paid} نقطة للمالك {owner_id}")
                            else:
                                logger.error(f"❌ فشل إعادة النقاط للمالك {owner_id}: {message}")
                    
                    # حذف القناة
                    del channels[channel_id]
                    removed_channels.append(channel_info)
                    logger.warning(f"🗑️ تم حذف القناة {channel_username} - البوت لم يعد مشرفاً")
                    
            except Exception as e:
                logger.error(f"❌ خطأ في فحص إشراف البوت للقناة {channel_username}: {e}")
        
        if removed_channels:
            data["channels"] = channels
            save_data(data, backup=False)
            
            # إرسال تقرير للمالك
            report_msg = (
                f"⚠️ تقرير فحص إشراف البوت\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"📅 التاريخ: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"🔍 تم فحص: {len(data.get('channels', {})) + len(removed_channels)} قناة\n"
                f"🗑️ تم حذف: {len(removed_channels)} قناة\n"
                f"✅ القنوات النشطة: {len(data.get('channels', {}))}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📋 القنوات المحذوفة:\n"
            )
            
            for i, chan in enumerate(removed_channels, 1):
                report_msg += f"{i}. @{chan['username']} - المالك: {chan['owner']}\n"
            
            await send_to_admin(bot, report_msg)
        
        return len(removed_channels)
        
    except Exception as e:
        logger.error(f"❌ خطأ كبير في check_and_remove_channel_if_bot_not_admin: {e}")
        return 0

async def handle_admin_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة رسائل الإدمن"""
    user_id = str(update.message.from_user.id)

    if context.user_data.get("buying"):
        return

    if not is_admin(update.message.from_user.id):
        return
    
    text = update.message.text
    
    if "admin_action" in context.user_data:
        action = context.user_data["admin_action"]
        
        try:
            if action == "user_info":
                await update.message.reply_text("⏳ جاري البحث عن المستخدم...")
                
                target = text.replace("@", "").strip()
                target_uid = None
                
                if target.isdigit():
                    users_data = load_users()
                    if target in users_data:
                        target_uid = target
                else:
                    target_uid = find_user_by_username(target)
                
                if target_uid:
                    user_data = get_user_data(target_uid)
                    data = load_data()
                    
                    daily_gift = user_data.get("daily_gift", {})
                    
                    ban_status = "✅ نشط" if target_uid not in data.get("banned_users", []) else "🚫 محظور"
                    mute_status, mute_until = is_muted(target_uid)
                    mute_status_text = "✅ غير مكتوم" 
                    if mute_status:
                        mute_status_text = f"🔇 مكتوم حتى: {mute_until if mute_until else 'دائم'}"
                    
                    info_text = (
                        f"👤 معلومات المستخدم الكاملة:\n\n"
                        f"🆔 ID: {target_uid}\n"
                        f"👤 اليوزر: @{user_data.get('username', 'بدون')}\n"
                        f"📛 الاسم: {user_data.get('first_name', '')} {user_data.get('last_name', '')}\n"
                        f"🎯 النقاط الحالية: {user_data.get('points', 0)}\n"
                        f"💰 مجموع الربح: {user_data.get('total_earned', 0)}\n"
                        f"💸 مجموع الصرف: {user_data.get('total_spent', 0)}\n"
                        f"🔗 عدد الدعوات: {user_data.get('invites', 0)}\n"
                        f"📅 تاريخ الانضمام: {user_data.get('first_join', '')}\n"
                        f"🔄 آخر نشاط: {user_data.get('last_active', '')}\n"
                        f"🚫 الحالة: {ban_status}\n"
                        f"🔇 الكتم: {mute_status_text}\n"
                        f"🛒 عدد الطلبات: {len(user_data.get('orders', []))}\n"
                        f"📢 عدد القنوات: {len(user_data.get('bought_channels', {}))}\n"
                        f"📊 انضمامات نشطة: {len(user_data.get('active_subscriptions', []))}\n"
                        f"🎁 الهدايا اليومية: {daily_gift.get('total_claimed', 0)} مرة\n"
                        f"📈 سلسلة الهدايا: {daily_gift.get('streak', 0)} يوم\n\n"
                    )
                    
                    await update.message.reply_text(info_text, parse_mode="HTML")
                else:
                    await update.message.reply_text("❌ المستخدم غير موجود في قاعدة البيانات.")
                
                del context.user_data["admin_action"]
                
            elif action == "broadcast":
                await update.message.reply_text("⏳ جاري إرسال الرسالة للجميع...")
                
                broadcast_msg = text
                users_data = load_users()
                sent_count = 0
                failed_count = 0
                total_users = len(users_data)
                
                for i, uid in enumerate(users_data.keys(), 1):
                    try:
                        await context.bot.send_message(
                            int(uid), 
                            f"📢 إشعار من الإدارة:\n\n{broadcast_msg}", 
                            parse_mode="HTML"
                        )
                        sent_count += 1
                        
                        if i % 50 == 0:
                            await update.message.reply_text(
                                f"📤 تم إرسال {i}/{total_users}..."
                            )
                        
                        time.sleep(0.1)
                        
                    except Exception:
                        failed_count += 1
                
                await update.message.reply_text(
                    f"✅ تم إكمال الإرسال!\n\n"
                    f"✅ تم الإرسال بنجاح: {sent_count}\n"
                    f"❌ فشل الإرسال: {failed_count}\n"
                    f"📊 الإجمالي: {total_users}",
                    parse_mode="HTML"
                )
                del context.user_data["admin_action"]
                
            elif action == "give_points":
                parts = text.split()
                if len(parts) < 2:
                    await update.message.reply_text("❌ تنسيق خاطئ. أرسل: يوزر/ID عدد")
                    return
                
                target_input = parts[0]
                amount = int(parts[1])
                target_uid = None
                
                if target_input.isdigit():
                    users_data = load_users()
                    if target_input in users_data:
                        target_uid = target_input
                else:
                    target_uid = find_user_by_username(target_input)
                
                if target_uid:
                    success, message = safe_add_points(target_uid, amount, "add", "admin_give_points")
                    if not success:
                        await update.message.reply_text(f"❌ {message}")
                        return
                    
                    user_data = get_user_data(target_uid)
                    
                    await update.message.reply_text(
                        f"✅ تم إضافة النقاط:\n\n"
                        f"👤 المستخدم: @{user_data.get('username', target_uid)}\n"
                        f"💰 المبلغ: {amount} نقطة\n"
                        f"🎯 النقاط الآن: {user_data['points']}",
                        parse_mode="HTML"
                    )
                    
                    try:
                        await context.bot.send_message(
                            int(target_uid),
                            f"🎉 مكافأة من الإدارة!\n\n"
                            f"💰 حصلت على: {amount} نقطة\n"
                            f"🎯 نقاطك الآن: {user_data['points']}",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
                else:
                    await update.message.reply_text("❌ المستخدم غير موجود.")
                
                del context.user_data["admin_action"]
                
            elif action == "take_points":
                parts = text.split()
                if len(parts) < 2:
                    await update.message.reply_text("❌ تنسيق خاطئ. أرسل: يوزر/ID عدد")
                    return
                
                target_input = parts[0]
                amount = int(parts[1])
                target_uid = None
                
                if target_input.isdigit():
                    users_data = load_users()
                    if target_input in users_data:
                        target_uid = target_input
                else:
                    target_uid = find_user_by_username(target_input)
                
                if target_uid:
                    success, message = safe_add_points(target_uid, amount, "subtract", "admin_take_points")
                    if not success:
                        await update.message.reply_text(f"❌ {message}")
                        return
                    
                    user_data = get_user_data(target_uid)
                    
                    await update.message.reply_text(
                        f"✅ تم خصم النقاط:\n\n"
                        f"👤 المستخدم: @{user_data.get('username', target_uid)}\n"
                        f"💸 المبلغ: {amount} نقطة\n"
                        f"🎯 النقاط الآن: {user_data['points']}",
                        parse_mode="HTML"
                    )
                else:
                    await update.message.reply_text("❌ المستخدم غير موجود.")
                
                del context.user_data["admin_action"]
                
            elif action == "ban_user":
                target = text.replace("@", "").strip()
                target_uid = None
                
                if target.isdigit():
                    users_data = load_users()
                    if target in users_data:
                        target_uid = target
                else:
                    target_uid = find_user_by_username(target)
                
                if target_uid:
                    data = load_data()
                    if target_uid not in data["banned_users"]:
                        data["banned_users"].append(target_uid)
                        save_data(data)
                        
                        user_data = get_user_data(target_uid)
                        
                        await update.message.reply_text(
                            f"✅ تم حظر المستخدم:\n\n"
                            f"👤 اليوزر: @{user_data.get('username', target_uid)}\n"
                            f"🆔 ID: {target_uid}\n"
                            f"📛 الاسم: {user_data.get('first_name', '')}\n"
                            f"📅 وقت الحظر: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                            parse_mode="HTML"
                        )
                        
                        try:
                            await context.bot.send_message(
                                int(target_uid),
                                "🚫 تم حظرك من استخدام البوت!\n\n"
                                "لقد تم حظرك من قبل الإدارة.",
                                parse_mode="HTML"
                            )
                        except:
                            pass
                    else:
                        await update.message.reply_text("⚠️ هذا المستخدم محظور بالفعل.")
                else:
                    await update.message.reply_text("❌ المستخدم غير موجود.")
                
                del context.user_data["admin_action"]
                
            elif action == "unban_user":
                target = text.replace("@", "").strip()
                target_uid = None
                
                if target.isdigit():
                    target_uid = target
                else:
                    target_uid = find_user_by_username(target)
                
                if target_uid:
                    data = load_data()
                    if target_uid in data["banned_users"]:
                        data["banned_users"].remove(target_uid)
                        save_data(data)
                        
                        users_data = load_users()
                        username = users_data.get(target_uid, {}).get("username", target_uid)
                        
                        await update.message.reply_text(
                            f"✅ تم فك حظر المستخدم:\n\n"
                            f"👤 اليوزر: @{username}\n"
                            f"🆔 ID: {target_uid}\n"
                            f"📅 وقت فك الحظر: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                            parse_mode="HTML"
                        )
                        
                        try:
                            await context.bot.send_message(
                                int(target_uid),
                                "✅ تم فك حظرك من البوت!\n\n"
                                "يمكنك الآن استخدام البوت مرة أخرى.",
                                parse_mode="HTML"
                            )
                        except:
                            pass
                    else:
                        await update.message.reply_text("❌ هذا المستخدم غير محظور.")
                
                del context.user_data["admin_action"]
                
            elif action == "mute_user":
                parts = text.split()
                if len(parts) < 2:
                    await update.message.reply_text("❌ تنسيق خاطئ. أرسل: يوزر/ID وقت_بالثواني [سبب]")
                    return
                
                target_input = parts[0]
                mute_seconds = int(parts[1])
                reason = " ".join(parts[2:]) if len(parts) > 2 else "بدون سبب"
                
                target_uid = None
                
                if target_input.isdigit():
                    users_data = load_users()
                    if target_input in users_data:
                        target_uid = target_input
                else:
                    target_uid = find_user_by_username(target_input)
                
                if target_uid:
                    if is_admin(int(target_uid)):
                        await update.message.reply_text("❌ لا يمكن كتم أدمن!")
                        return
                    
                    if is_banned(int(target_uid)):
                        await update.message.reply_text("⚠️ المستخدم محظور بالفعل!")
                        return
                    
                    mute_info = add_muted_user(target_uid, mute_seconds, reason)
                    
                    user_data = get_user_data(target_uid)
                    
                    duration_text = "دائم" if mute_seconds == 0 else format_time(mute_seconds)
                    mute_until_text = mute_info.get("until", "غير محدد")
                    
                    try:
                        await context.bot.send_message(
                            int(target_uid),
                            f"🔇 تم كتمك من البوت!\n\n"
                            f"⏰ المدة: {duration_text}\n"
                            f"📅 ينتهي في: {mute_until_text}\n"
                            f"📝 السبب: {reason}\n\n"
                            f"🚫 لن تتمكن من استخدام البوت حتى انتهاء المدة.",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
                    
                    await update.message.reply_text(
                        f"✅ تم كتم المستخدم:\n\n"
                        f"👤 المستخدم: @{user_data.get('username', target_uid)}\n"
                        f"⏰ المدة: {duration_text}\n"
                        f"📅 ينتهي في: {mute_until_text}\n"
                        f"📝 السبب: {reason}",
                        parse_mode="HTML"
                    )
                else:
                    await update.message.reply_text("❌ المستخدم غير موجود.")
                
                del context.user_data["admin_action"]
                
            elif action == "unmute_user":
                target = text.replace("@", "").strip()
                target_uid = None
                
                if target.isdigit():
                    target_uid = target
                else:
                    target_uid = find_user_by_username(target)
                
                if target_uid:
                    is_muted_status, mute_until = is_muted(target_uid)
                    
                    if not is_muted_status:
                        await update.message.reply_text("❌ هذا المستخدم غير مكتوم!")
                        return
                    
                    if remove_muted_user(target_uid):
                        await update.message.reply_text(
                            f"✅ تم فك كتم المستخدم:\n\n"
                            f"🆔 ID: {target_uid}\n"
                            f"📅 وقت فك الكتم: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                            parse_mode="HTML"
                        )
                        
                        try:
                            await context.bot.send_message(
                                int(target_uid),
                                "🔊 تم فك كتمك من البوت!\n\n"
                                "✅ يمكنك الآن استخدام البوت مرة أخرى.",
                                parse_mode="HTML"
                            )
                        except Exception:
                            pass
                
                del context.user_data["admin_action"]
                
            elif action == "add_channel":
                parts = text.split()
                if len(parts) < 2:
                    await update.message.reply_text("❌ تنسيق خاطئ. أرسل: @channel عدد")
                    return
                
                channel_username = parts[0].replace("@", "").strip()
                members_count = int(parts[1])
                
                data = load_data()
                
                # التحقق من وجود قناة نشطة لنفس اليوزر
                existing_active_channel = None
                for cid, existing_channel in data.get("channels", {}).items():
                    if (existing_channel.get("username") == channel_username and 
                        not existing_channel.get("completed", False)):  # قناة نشطة غير مكتملة
                        existing_active_channel = (cid, existing_channel)
                        break

                if existing_active_channel:
                    # هناك قناة نشطة لنفس اليوزر
                    cid, chan_data = existing_active_channel
                    owner_id = chan_data.get("owner", "غير معروف")
                    
                    # الحصول على اسم المالك إذا كان مستخدم عادي
                    owner_name = owner_id
                    if owner_id != str(ADMIN_ID):
                        owner_data = get_user_data(owner_id)
                        owner_name = f"@{owner_data.get('username', owner_id)}"
                    
                    await update.message.reply_text(
                        f"❌ يوجد قناة نشطة لهذا اليوزر!\n\n"
                        f"📢 القناة: @{channel_username}\n"
                        f"👤 المالك: {owner_name}\n"
                        f"🆔 ID المالك: {owner_id}\n"
                        f"📊 التقدم: {chan_data.get('current', 0)}/{chan_data.get('required', 0)}\n"
                        f"🆔 المعرف: {cid}\n"
                        f"📅 تاريخ الإضافة: {chan_data.get('created_at', 'غير معروف')}\n\n"
                        f"💡 يجب:\n"
                        f"• الانتظار حتى تكتمل القناة\n"
                        f"• أو حذف القناة أولاً (باستخدام /admin_remove_channel)\n"
                        f"• أو إعادة تفعيلها إذا كانت مكتملة",
                        parse_mode="HTML"
                    )
                    del context.user_data["admin_action"]
                    return
                
                # البحث عن قناة مكتملة من الأدمن لنفس القناة
                existing_completed_channel = None
                for cid, existing_channel in data.get("channels", {}).items():
                    if (existing_channel.get("username") == channel_username and 
                        existing_channel.get("owner") == str(ADMIN_ID) and 
                        existing_channel.get("completed", False)):
                        existing_completed_channel = (cid, existing_channel)
                        break
                
                if existing_completed_channel:
                    # إعادة استخدام القناة المكتملة
                    channel_id, channel_data = existing_completed_channel
                    
                    channel_data.update({
                        "required": members_count,
                        "current": 0,
                        "completed": False,
                        "reuse_count": channel_data.get("reuse_count", 0) + 1,
                        "joined_users": [],
                        "reactivated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "admin_added": True
                    })
                    
                    data["channels"][channel_id] = channel_data
                    save_data(data)
                    
                    await update.message.reply_text(
                        f"🔄 تم إعادة تفعيل قناة الأدمن!\n\n"
                        f"📢 اليوزر: @{channel_username}\n"
                        f"👥 العدد المطلوب: {members_count} عضو\n"
                        f"💰 النقاط للمنضم: 3 نقاط\n"
                        f"🆔 المعرف: {channel_id}\n"
                        f"🔄 عدد مرات الاستخدام: {channel_data.get('reuse_count', 1)}\n"
                        f"📅 تاريخ الإضافة: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        parse_mode="HTML"
                    )
                else:
                    # قناة جديدة
                    channel_id = f"admin_channel_{int(time.time())}_{abs(hash(channel_username)) % 10000}"
                    
                    data["channels"][channel_id] = {
                        "username": channel_username,
                        "owner": str(ADMIN_ID),
                        "required": members_count,
                        "current": 0,
                        "completed": False,
                        "joined_users": [],
                        "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "admin_added": True,
                        "reuse_count": 0
                    }
                    
                    save_data(data)
                    
                    await update.message.reply_text(
                        f"✅ تم إضافة قناة جديدة:\n\n"
                        f"📢 اليوزر: @{channel_username}\n"
                        f"👥 العدد المطلوب: {members_count} عضو\n"
                        f"💰 النقاط للمنضم: 3 نقاط\n"
                        f"🆔 المعرف: {channel_id}\n"
                        f"📅 تاريخ الإضافة: {data['channels'][channel_id]['created_at']}",
                        parse_mode="HTML"
                    )
                del context.user_data["admin_action"]
                
            elif action == "add_force":
                channel_username = text.replace("@", "").strip()
                
                bot_is_admin = await check_bot_is_admin(context.bot, channel_username)
                
                if not bot_is_admin:
                    await update.message.reply_text(
                        f"❌ البوت ليس مشرفاً في هذه القناة!\n\n"
                        f"📢 @{channel_username}\n\n"
                        f"➕ أضف البوت كمشرف في القناة أولاً، ثم أعد المحاولة.",
                        parse_mode="HTML"
                    )
                    del context.user_data["admin_action"]
                    return
                
                data = load_data()
                if channel_username not in data.get("force_sub_channels", []):
                    data["force_sub_channels"].append(channel_username)
                    save_data(data)
                    
                    await update.message.reply_text(
                        f"✅ تم إضافة قناة اشتراك إجباري:\n\n"
                        f"🔒 اليوزر: @{channel_username}\n"
                        f"🤖 حالة البوت: مشرف ✓\n"
                        f"📊 عدد القنوات الإجبارية: {len(data['force_sub_channels'])}\n"
                        f"📅 تاريخ الإضافة: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        parse_mode="HTML"
                    )
                else:
                    await update.message.reply_text("⚠️ هذه القناة مضافة بالفعل.")
                
                del context.user_data["admin_action"]
                
            elif action == "add_code":
                parts = text.split()
                if len(parts) < 3:
                    await update.message.reply_text("❌ تنسيق خاطئ. أرسل: اسم نقاط مستخدمين")
                    return
                
                code_name = parts[0].upper()
                points = int(parts[1])
                max_uses = int(parts[2])
                
                data = load_data()
                
                if code_name in data.get("codes", {}):
                    await update.message.reply_text("⚠️ هذا الكود موجود بالفعل!")
                    return
                
                data["codes"][code_name] = {
                    "points": points,
                    "max_uses": max_uses,
                    "used_count": 0,
                    "used_by": [],
                    "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "created_by": str(ADMIN_ID)
                }
                
                save_data(data)
                
                await update.message.reply_text(
                    f"✅ تم إضافة كود جديد:\n\n"
                    f"🎟️ اسم الكود: {code_name}\n"
                    f"💰 عدد النقاط: {points}\n"
                    f"👥 عدد المستخدمين: {max_uses}\n"
                    f"📅 تاريخ الإنشاء: {data['codes'][code_name]['created_at']}\n\n"
                    f"💡 للاستخدام: /code {code_name}",
                    parse_mode="HTML"
                )
                del context.user_data["admin_action"]
                
            elif action == "remove_channel":
                channel_input = text.strip()
                
                data = load_data()
                removed_channels = []
                
                # البحث عن القنوات
                for cid, channel_data in data.get("channels", {}).items():
                    # إذا كان الإدخال يبدأ بـ @ فهو يوزر قناة
                    if channel_input.startswith("@"):
                        channel_username = channel_input.replace("@", "").strip()
                        if channel_data.get("username") == channel_username:
                            removed_channels.append({
                                "id": cid,
                                "username": channel_data.get("username"),
                                "owner": channel_data.get("owner"),
                                "progress": f"{channel_data.get('current', 0)}/{channel_data.get('required', 0)}",
                                "completed": channel_data.get("completed", False),
                                "created_at": channel_data.get("created_at", "غير معروف")
                            })
                    else:
                        # إذا كان معرف القناة مباشرة
                        if cid == channel_input:
                            removed_channels.append({
                                "id": cid,
                                "username": channel_data.get("username"),
                                "owner": channel_data.get("owner"),
                                "progress": f"{channel_data.get('current', 0)}/{channel_data.get('required', 0)}",
                                "completed": channel_data.get("completed", False),
                                "created_at": channel_data.get("created_at", "غير معروف")
                            })
                
                if removed_channels:
                    # حذف القنوات
                    for chan in removed_channels:
                        del data["channels"][chan["id"]]
                    
                    save_data(data)
                    
                    # بناء رسالة النتائج
                    result_text = f"✅ تم حذف {len(removed_channels)} قناة:\n\n"
                    for i, chan in enumerate(removed_channels, 1):
                        status = "✅ مكتملة" if chan["completed"] else "🟡 نشطة"
                        owner_name = chan["owner"]
                        if chan["owner"] != str(ADMIN_ID):
                            owner_data = get_user_data(chan["owner"])
                            owner_name = f"@{owner_data.get('username', chan['owner'])}"
                        
                        result_text += f"{i}. 📢 @{chan['username']}\n"
                        result_text += f"   {status}\n"
                        result_text += f"   👤 المالك: {owner_name}\n"
                        result_text += f"   📊 التقدم: {chan['progress']}\n"
                        result_text += f"   🆔 المعرف: {chan['id'][:15]}...\n"
                        result_text += f"   📅 تاريخ الإضافة: {chan['created_at']}\n\n"
                    
                    result_text += f"📅 تاريخ الحذف: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    
                    await update.message.reply_text(result_text, parse_mode="HTML")
                else:
                    await update.message.reply_text(
                        "❌ لا توجد قنوات تطابق البحث!\n\n"
                        "💡 يمكنك البحث بـ:\n"
                        "1. يوزر القناة (مثال: @TUX3T)\n"
                        "2. معرف القناة (مثال: order_12345678_1234567890)",
                        parse_mode="HTML"
                    )
                
                del context.user_data["admin_action"]
                
            elif action == "remove_force":
                channel_username = text.replace("@", "").strip()
                
                data = load_data()
                if channel_username in data.get("force_sub_channels", []):
                    data["force_sub_channels"].remove(channel_username)
                    save_data(data)
                    await update.message.reply_text(
                        f"✅ تم حذف قناة الإجباري:\n\n"
                        f"🔓 @{channel_username}\n"
                        f"📅 تاريخ الحذف: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        parse_mode="HTML"
                    )
                else:
                    await update.message.reply_text("❌ القناة غير موجودة في قائمة الإجباري.")
                
                del context.user_data["admin_action"]
                
            elif action == "remove_code":
                code_name = text.upper().strip()
                
                data = load_data()
                if code_name in data.get("codes", {}):
                    code_data = data["codes"][code_name]
                    del data["codes"][code_name]
                    save_data(data)
                    await update.message.reply_text(
                        f"✅ تم حذف الكود:\n\n"
                        f"🎟️ اسم الكود: {code_name}\n"
                        f"💰 عدد النقاط: {code_data.get('points', 0)}\n"
                        f"👥 عدد المستخدمين: {code_data.get('used_count', 0)}/{code_data.get('max_uses', 0)}\n"
                        f"📅 تاريخ الحذف: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                        parse_mode="HTML"
                    )
                else:
                    await update.message.reply_text("❌ الكود غير موجود.")
                
                del context.user_data["admin_action"]
                
        except ValueError:
            await update.message.reply_text("❌ الرقم الذي أدخلته غير صحيح!")
            if "admin_action" in context.user_data:
                del context.user_data["admin_action"]
        except Exception as e:
            await update.message.reply_text(f"❌ حدث خطأ: {str(e)}")
            if "admin_action" in context.user_data:
                del context.user_data["admin_action"]

# ===================== معالجة الرسائل العامة =====================

def mark_channel_as_left(user_id, channel_id, channel_data=None):
    """تحديد القناة كمتروكة من قبل المستخدم - نسخة محسنة"""
    try:
        if channel_data is None:
            data = load_data(force_reload=True)
            channel_data = data.get("channels", {}).get(channel_id, {})
        
        user_data = get_user_data(user_id, force_reload=True)
        
        joined_channels = user_data.get("joined_channels", {})
        updates = {}
        
        if channel_id in joined_channels:
            joined_channels[channel_id]["left"] = True
            joined_channels[channel_id]["left_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # تحديد نوع المغادرة بناءً على حالة القناة
            is_completed = channel_data.get("completed", False)
            current_round = channel_data.get("reuse_count", 0)
            
            if is_completed:
                # قناة مكتملة - علامة خاصة
                joined_channels[channel_id]["left_completed"] = True
                joined_channels[channel_id]["completed_round"] = current_round
                joined_channels[channel_id]["completed_at"] = channel_data.get("completed_at", "")
                
                logger.info(f"📦 المستخدم {user_id} غادر قناة مكتملة: {channel_id} (الجولة {current_round})")
            else:
                logger.info(f"📤 المستخدم {user_id} غادر قناة قيد التجميع: {channel_id}")
            
            # إضافة إلى temp_left_channels (للقنوات النشطة والمكتملة)
            temp_left = user_data.get("temp_left_channels", [])
            if channel_id not in temp_left:
                temp_left.append(channel_id)
                updates["temp_left_channels"] = temp_left
            
            # حفظ joined_channels المحدثة
            updates["joined_channels"] = joined_channels
            
        else:
            # لا توجد بيانات انضمام سابقة
            if not channel_data.get("completed", False):
                # قناة قيد التجميع فقط (بدون انضمام سابق)
                temp_left = user_data.get("temp_left_channels", [])
                if channel_id not in temp_left:
                    temp_left.append(channel_id)
                    updates["temp_left_channels"] = temp_left
                    
                logger.info(f"📝 تمت إضافة {channel_id} لـ temp_left_channels للمستخدم {user_id}")
        
        # إزالة من القنوات النشطة (إن وجدت)
        active_subscriptions = user_data.get("active_subscriptions", [])
        if channel_id in active_subscriptions:
            active_subscriptions = [c for c in active_subscriptions if c != channel_id]
            updates["active_subscriptions"] = active_subscriptions
            logger.info(f"🗑️ تمت إزالة {channel_id} من active_subscriptions للمستخدم {user_id}")
        
        # إزالة من القنوات المتروكة القديمة (للتوافق)
        old_left = user_data.get("left_channels", [])
        if channel_id in old_left:
            old_left = [c for c in old_left if c != channel_id]
            updates["left_channels"] = old_left
        
        # إزالة من القنوات المتروكة نهائياً (إن وجدت)
        permanent_left = user_data.get("permanent_left_channels", [])
        if channel_id in permanent_left:
            permanent_left = [c for c in permanent_left if c != channel_id]
            updates["permanent_left_channels"] = permanent_left
        
        # إزالة من left_completed_channels (إن وجدت)
        left_completed = user_data.get("left_completed_channels", [])
        if channel_id in left_completed:
            left_completed = [c for c in left_completed if c != channel_id]
            updates["left_completed_channels"] = left_completed
        
        # تنفيذ التحديثات
        if updates:
            success = update_user_data(user_id, updates, "mark_channel_left")
            if success:
                logger.info(f"✅ تم وضع علامة المغادرة للمستخدم {user_id} من القناة {channel_id}")
                return True
            else:
                logger.error(f"❌ فشل تحديث بيانات المغادرة للمستخدم {user_id}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في mark_channel_as_left للمستخدم {user_id}: {e}")
        import traceback
        traceback.print_exc()
        return False

async def decrease_channel_counter(bot, user_id, channel_id, channel_data=None, penalty_amount=5):
    """
    تقليل عداد القناة عند مغادرة المستخدم
    
    Features:
    - تقليل العداد بمقدار 1
    - منع العداد من أن يصبح سالباً
    - تسجيل المغادرة في السجل
    - إزالة المستخدم من joined_users
    - إلغاء الاكتمال إذا لزم الأمر
    
    Args:
        bot: كائن البوت
        user_id: معرّف المستخدم
        channel_id: معرّف القناة
        channel_data: بيانات القناة (اختياري)
        penalty_amount: مقدار الخصم (افتراضي 5)
    
    Returns:
        tuple: (success: bool, new_counter: int, message: str)
    """
    try:
        data = load_data(force_reload=True)
        
        # التحقق من وجود القناة
        if channel_id not in data.get("channels", {}):
            return False, 0, "القناة غير موجودة"
        
        channel = data["channels"][channel_id]
        current_count = channel.get("current", 0)
        
        # ✅ تقليل العداد (لا يقل عن 0)
        new_count = max(0, current_count - 1)
        
        # تحديث بيانات القناة
        channel["current"] = new_count
        channel["last_activity"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # تسجيل المغادرة في السجل
        if "leave_history" not in channel:
            channel["leave_history"] = []
        
        channel["leave_history"].append({
            "user_id": user_id,
            "left_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "previous_count": current_count,
            "new_count": new_count,
            "penalty_applied": penalty_amount,
            "channel_username": channel.get("username", "unknown")
        })
        
        # إزالة المستخدم من joined_users
        if "joined_users" in channel:
            original_length = len(channel["joined_users"])
            channel["joined_users"] = [
                u for u in channel["joined_users"] 
                if str(u.get("user_id", "")) != str(user_id)
            ]
            removed = original_length - len(channel["joined_users"])
            if removed > 0:
                logger.info(f"🗑️ تمت إزالة {removed} سجل للمستخدم {user_id} من joined_users")
        
        # إلغاء الاكتمال إذا أصبح العداد أقل من المطلوب
        required = channel.get("required", 0)
        if channel.get("completed", False) and new_count < required:
            channel["completed"] = False
            channel["uncompleted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            channel["uncompleted_reason"] = f"user_left:{user_id}"
            logger.warning(
                f"⚠️ تم إلغاء اكتمال القناة {channel.get('username')} - "
                f"العداد: {new_count}/{required} (مغادرة المستخدم {user_id})"
            )
        
        # حفظ التحديثات
        data["channels"][channel_id] = channel
        
        if save_data(data, backup=False):
            logger.info(
                f"✅ تم تقليل عداد القناة {channel.get('username')}: "
                f"{current_count} → {new_count} (المستخدم {user_id})"
            )
            return True, new_count, f"تم تقليل العداد من {current_count} إلى {new_count}"
        else:
            logger.error(f"❌ فشل حفظ بيانات القناة {channel_id}")
            return False, current_count, "فشل حفظ البيانات"
            
    except Exception as e:
        logger.error(f"❌ خطأ في decrease_channel_counter للقناة {channel_id}: {e}")
        import traceback
        traceback.print_exc()
        return False, 0, str(e)
        
def get_channel_counter_stats(channel_id):
    """
    الحصول على إحصائيات العداد للقناة
    
    Args:
        channel_id: معرّف القناة
    
    Returns:
        dict: إحصائيات مفصلة أو None إذا فشل
    """
    try:
        data = load_data(force_reload=True)
        
        if channel_id not in data.get("channels", {}):
            return None
        
        channel = data["channels"][channel_id]
        
        stats = {
            "current": channel.get("current", 0),
            "required": channel.get("required", 0),
            "percentage": (channel.get("current", 0) / max(channel.get("required", 1), 1)) * 100,
            "completed": channel.get("completed", False),
            "total_joins": len(channel.get("joined_users", [])),
            "total_leaves": len(channel.get("leave_history", [])),
            "total_returns": len(channel.get("return_history", [])),
            "net_change": len(channel.get("joined_users", [])) - len(channel.get("leave_history", [])),
            "channel_username": channel.get("username", "unknown"),
            "owner": channel.get("owner", "unknown")
        }
        
        return stats
        
    except Exception as e:
        logger.error(f"خطأ في get_channel_counter_stats: {e}")
        return None


def cleanup_channel_data():
    """تنظيف بيانات القنوات من الحقول غير المتسقة"""
    try:
        data = load_data(force_reload=True)
        channels = data.get("channels", {})
        cleaned_count = 0
        
        for channel_id, channel_data in channels.items():
            needs_update = False
            
            # 1. إذا كانت completed=false ولكن فيها completed_at
            if not channel_data.get("completed", False) and "completed_at" in channel_data:
                del channel_data["completed_at"]
                needs_update = True
                logger.info(f"🧹 حذف completed_at من {channel_data.get('username')} (completed=false)")
            
            # 2. إذا كانت completed=true ولكن current < required
            if channel_data.get("completed", False):
                current = channel_data.get("current", 0)
                required = channel_data.get("required", 1)
                if current < required:
                    channel_data["completed"] = False
                    if "completed_at" in channel_data:
                        del channel_data["completed_at"]
                    needs_update = True
                    logger.info(f"🔧 صححت completed من true إلى false لـ {channel_data.get('username')} ({current}/{required})")
            
            if needs_update:
                data["channels"][channel_id] = channel_data
                cleaned_count += 1
        
        if cleaned_count > 0:
            save_data(data, backup=False)
            logger.info(f"✅ تم تنظيف {cleaned_count} قناة")
        
        return cleaned_count
        
    except Exception as e:
        logger.error(f"❌ خطأ في cleanup_channel_data: {e}")
        return 0


# الإصلاح المطلوب لدالة should_channel_be_shown_to_user

def should_channel_be_shown_to_user(user_id, channel_id):
    """التحقق مما إذا كان يجب عرض القناة للمستخدم مع مراعاة إعادة التفعيل"""
    user_data = get_user_data(user_id)
    data = load_data()
    
    if channel_id not in data.get("channels", {}):
        return False
    
    channel_data = data["channels"][channel_id]
    
    # التحقق من اكتمال القناة
    if channel_data.get("completed", False):
        return False
    
    # التحقق إذا كان المستخدم صاحب القناة
    if str(user_id) == channel_data.get("owner"):
        return False
    
    # التحقق إذا كان المستخدم أدمن أضاف القناة
    if channel_data.get("owner") == str(ADMIN_ID) and str(user_id) in data.get("admins", []):
        return False
    
    # التحقق من joined_channels
    joined_channels = user_data.get("joined_channels", {})
    if channel_id in joined_channels:
        join_info = joined_channels[channel_id]
        current_round = channel_data.get("reuse_count", 0)
        user_round = join_info.get("round", 0)
        
        # تحقق 1: إذا كان منضماً حالياً ولم يغادر
        if join_info.get("verified", False) and not join_info.get("left", False):
            # نفس الجولة → لا تظهر
            if user_round == current_round:
                return False
            # جولة جديدة → تظهر
            elif current_round > user_round:
                return True
            else:
                return False
        
        # تحقق 2: إذا غادر قناة مكتملة
        if join_info.get("left_completed", False):
            completed_round = join_info.get("completed_round", 0)
            
            # إذا أعيد تفعيل القناة (جولة جديدة)
            if current_round > completed_round:
                return True  # ✅ تظهر له النسخة الجديدة
            else:
                return False  # ❌ نفس النسخة المكتملة
        
        # 🔥 تحقق 3: إذا غادر قناة نشطة (قيد التجميع) 🔥
        # هذا هو الإصلاح الرئيسي!
        if join_info.get("left", False):
            # نفس الجولة → يمكنه العودة ✅
            if user_round == current_round:
                return True  # ✅ يمكنه العودة للقناة التي غادرها!
            # جولة جديدة → بالتأكيد يمكنه الانضمام
            elif current_round > user_round:
                return True
            else:
                return False
        
        # تحقق 4: إعادة التفعيل العادية
        reactivated_at = channel_data.get("reactivated_at")
        if reactivated_at and "joined_at" in join_info:
            try:
                join_time = datetime.strptime(join_info["joined_at"], "%Y-%m-%d %H:%M:%S")
                reactivate_time = datetime.strptime(reactivated_at, "%Y-%m-%d %H:%M:%S")
                
                # إذا انضم قبل إعادة التفعيل
                if join_time < reactivate_time:
                    if join_info.get("left", False):
                        return True  # ✅ غادر النسخة القديمة → تظهر له الجديدة
                    else:
                        return False  # ❌ لا يزال منضم للنسخة القديمة
                
                # إذا انضم بعد إعادة التفعيل
                else:
                    if join_info.get("verified", False) and not join_info.get("left", False):
                        return False  # ❌ منضم للنسخة الجديدة
                    elif join_info.get("left", False):
                        return True  # ✅ غادر → يمكنه العودة
                        
            except Exception:
                pass
    
    # التحقق من temp_left_channels
    temp_left = user_data.get("temp_left_channels", [])
    if channel_id in temp_left:
        return True  # ✅ في القائمة المؤقتة → تظهر
    
    # التحقق من permanent_left_channels (يجب أن يكون فارغاً الآن)
    permanent_left = user_data.get("permanent_left_channels", [])
    if channel_id in permanent_left:
        # نقوم بتنظيفها لأنها غير مستخدمة الآن
        updates = {"permanent_left_channels": [c for c in permanent_left if c != channel_id]}
        update_user_data(user_id, updates, "clean_permanent_left")
        return True  # ✅ بعد التنظيف، تظهر
    
    # التحقق من القنوات النشطة
    active_subs = user_data.get("active_subscriptions", [])
    if channel_id in active_subs:
        # تحقق إضافي: هل غادر فعلاً؟
        if channel_id in joined_channels:
            if joined_channels[channel_id].get("left", False):
                return True  # ✅ غادر → تظهر
        return False  # ❌ في القنوات النشطة ولم يغادر
    
    # ✅ كل الاختبارات فشلت → يعني يمكنه رؤيتها
    return True


async def handle_general_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة الرسائل العامة"""
    user_id = str(update.message.from_user.id)
    text = update.message.text
    
    if is_banned(update.message.from_user.id):
        return
    
    is_user_muted, mute_until = is_muted(user_id)
    if is_user_muted:
        await update.message.reply_text(
            f"🔇 أنت مكتوم من استخدام البوت!\n\n"
            f"⏰ ينتهي الكتم في: {mute_until if mute_until else 'دائم'}",
            parse_mode="HTML"
        )
        return
    
    if is_admin(update.message.from_user.id) and "admin_action" in context.user_data:
        return
    
    if "buying" in context.user_data:
        return

async def handle_channel_purchase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة شراء قناة"""
    user = update.message.from_user
    user_id = str(user.id)
    
    # إذا كان أدمن وفي وضع admin_action، لا نتعامل معه هنا
    if is_admin(user.id) and "admin_action" in context.user_data:
        # الأدمن في وضع إداري (حذف، إضافة، إلخ)
        # لا نتعامل مع رسالته هنا، بل تذهب إلى handle_admin_message
        return

    if "buying" not in context.user_data:
        return

    text = update.message.text.strip()
    buying = context.user_data["buying"]

    if not text.startswith("@") or len(text) < 4:
        await update.message.reply_text(
            "❌ أرسل يوزر القناة بشكل صحيح\nمثال: @channel_username"
        )
        return

    channel_username = text.replace("@", "").strip()
    transaction_id = buying.get("transaction_id", f"purchase_{user_id}_{int(time.time() * 1000)}")
    
    # قفل للشراء
    lock_key = f"purchase_{user_id}_{channel_username}"
    _store_locks.setdefault(lock_key, threading.Lock())
    
    with _store_locks[lock_key]:
        # التحقق من إشراف البوت
        try:
            bot_member = await context.bot.get_chat_member(
                chat_id=f"@{channel_username}",
                user_id=context.bot.id
            )

            if bot_member.status not in ("administrator", "creator"):
                await update.message.reply_text(
                    f"❌ البوت ليس مشرفاً في القناة!\n\n"
                    f"📢 @{channel_username}\n\n"
                    f"➕ يجب عليك أولاً:\n"
                    f"1. أضف البوت كمشرف في القناة\n"
                    f"2. أعطه كل الصلاحيات\n"
                    f"3. أعد إرسال يوزر القناة",
                    parse_mode="HTML"
                )
                return

        except Exception as e:
            error_msg = str(e).lower()
            if "forbidden" in error_msg or "kicked" in error_msg:
                await update.message.reply_text(
                    f"❌ البوت ليس مشرفاً في القناة!\n\n"
                    f"📢 @{channel_username}\n\n"
                    f"➕ أضف البوت كمشرف في القناة أولاً",
                    parse_mode="HTML"
                )
            else:
                await update.message.reply_text(
                    f"❌ حدث خطأ!\n\n"
                    f"تأكد من:\n"
                    f"• القناة عامة\n"
                    f"• اليوزر صحيح\n"
                    f"• البوت مضاف كمشرف",
                    parse_mode="HTML"
                )
            return

        user_data = get_user_data(user_id, force_reload=True)

        if user_data["points"] < buying["points"]:
            await update.message.reply_text(
                f"❌ نقاطك غير كافية!\n"
                f"تحتاج {buying['points']} نقطة"
            )
            return

        data = load_data()
        
        # منع صاحب القناة النشطة من شراء أعضاء لها
        active_user_channels = []
        
        for cid, chan_data in data.get("channels", {}).items():
            if (chan_data.get("username") == channel_username and 
                chan_data.get("owner") == user_id and 
                not chan_data.get("completed", False)):  # قناة نشطة غير مكتملة
                active_user_channels.append(cid)
        
        if active_user_channels:
            # الحصول على معلومات القنوات النشطة
            active_channels_info = []
            for cid in active_user_channels:
                chan_data = data["channels"][cid]
                progress = f"{chan_data.get('current', 0)}/{chan_data.get('required', 0)}"
                created_at = chan_data.get('created_at', 'غير معروف')
                active_channels_info.append(f"• {progress} - {created_at}")
            
            await update.message.reply_text(
                f"❌ لا يمكنك شراء أعضاء لهذه القناة!\n\n"
                f"📢 القناة: @{channel_username}\n"
                f"📊 لديك {len(active_user_channels)} قناة نشطة لهذا اليوزر:\n"
                f"{chr(10).join(active_channels_info)}\n\n"
                f"💡 يجب عليك:\n"
                f"1. الانتظار حتى تكتمل القنوات الحالية\n"
                f"2. أو إعادة تفعيل قناة مكتملة (إذا كانت هناك قناة مكتملة)",
                parse_mode="HTML"
            )
            return

        channels = data.get("channels", {})
        
        # البحث عن قناة مكتملة من نفس المستخدم لنفس القناة
        existing_completed_channel = None
        for channel_id, channel_data in channels.items():
            if (channel_data.get("username") == channel_username and 
                channel_data.get("owner") == user_id and 
                channel_data.get("completed", False)):
                existing_completed_channel = (channel_id, channel_data)
                break
        
        if existing_completed_channel:
            # إعادة استخدام القناة المكتملة
            channel_id, channel_data = existing_completed_channel
            
            # خصم النقاط
            success, message = safe_add_points(
                user_id, 
                buying["points"], 
                "subtract", 
                "channel_reuse_purchase",
                transaction_id
            )
            
            if not success:
                await update.message.reply_text(f"❌ {message}")
                return
            
            # تنظيف بيانات جميع المستخدمين السابقين
            users_data = load_users()
            cleaned_users = 0
            
            for uid, user_info in users_data.items():
                try:
                    cleaned = False
                    
                    if "left_completed_channels" in user_info and channel_id in user_info["left_completed_channels"]:
                        user_info["left_completed_channels"].remove(channel_id)
                        cleaned = True
                    
                    if "permanent_left_channels" in user_info and channel_id in user_info["permanent_left_channels"]:
                        user_info["permanent_left_channels"].remove(channel_id)
                        cleaned = True
                    
                    if "temp_left_channels" in user_info and channel_id in user_info["temp_left_channels"]:
                        user_info["temp_left_channels"].remove(channel_id)
                        cleaned = True
                    
                    if "left_channels" in user_info and channel_id in user_info["left_channels"]:
                        user_info["left_channels"].remove(channel_id)
                        cleaned = True
                    
                    if "joined_channels" in user_info and channel_id in user_info["joined_channels"]:
                        del user_info["joined_channels"][channel_id]
                        cleaned = True
                    
                    if "active_subscriptions" in user_info and channel_id in user_info["active_subscriptions"]:
                        user_info["active_subscriptions"] = [c for c in user_info["active_subscriptions"] if c != channel_id]
                        cleaned = True
                    
                    if cleaned:
                        cleaned_users += 1
                        users_data[uid] = user_info
                        
                except Exception as e:
                    logger.error(f"خطأ في تنظيف بيانات المستخدم {uid}: {e}")
            
            if cleaned_users > 0:
                save_users(users_data, backup=False)
                logger.info(f"🧹 تم تنظيف بيانات {cleaned_users} مستخدم للقناة {channel_username}")
            
            # تحديث بيانات القناة
            channel_data.update({
                "required": buying["members"],
                "current": 0,
                "completed": False,
                "reuse_count": channel_data.get("reuse_count", 0) + 1,
                "joined_users": [],
                "reactivated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "last_activity": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "previous_completion": channel_data.get("completed_at"),
                "reactivated_by": user_id,
                "admin_added": channel_data.get("admin_added", False)
            })
            
            order_id = channel_id
            
            # حفظ الطلب
            user_data.setdefault("orders", []).append({
                "order_id": order_id,
                "channel": channel_username,
                "members": buying["members"],
                "points": buying["points"],
                "status": "إعادة تفعيل",
                "current": 0,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "reuse_number": channel_data.get("reuse_count", 1),
                "transaction_id": transaction_id,
                "reactivated_from_completed": True
            })
            
            update_user_data(user_id, {"orders": user_data["orders"]}, "channel_reuse_purchase", transaction_id)
            
            data["channels"][channel_id] = channel_data
            save_data(data)
            
            update_system_stats("total_purchases", increment=1)
            
            await update.message.reply_text(
                f"🔄 تم إعادة تفعيل القناة المكتملة!\n\n"
                f"📢 القناة: @{channel_username}\n"
                f"👥 العدد المطلوب: {buying['members']}\n"
                f"💰 المدفوع: {buying['points']} نقطة\n"
                f"⭐ رصيدك الآن: {user_data['points'] - buying['points']}\n"
                f"🆔 رقم الطلب: {order_id}\n"
                f"🔄 عدد المرات المستخدمة: {channel_data.get('reuse_count', 1)}\n"
                f"🧹 تم تنظيف بيانات {cleaned_users} مستخدم سابق\n\n"
                f"🚀 بدأ التجميع مرة أخرى للجميع!",
                parse_mode="HTML"
            )
            
        else:
            # شراء جديد
            # خصم النقاط
            success, message = safe_add_points(
                user_id, 
                buying["points"], 
                "subtract", 
                "channel_purchase",
                transaction_id
            )
            
            if not success:
                await update.message.reply_text(f"❌ {message}")
                return

            order_id = f"order_{user_id}_{int(time.time())}"

            # حفظ الطلب
            user_data.setdefault("orders", []).append({
                "order_id": order_id,
                "channel": channel_username,
                "members": buying["members"],
                "points": buying["points"],
                "status": "قيد التنفيذ",
                "current": 0,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "transaction_id": transaction_id
            })

            update_user_data(user_id, {"orders": user_data["orders"]}, "channel_purchase", transaction_id)

            # حفظ القناة
            data["channels"][order_id] = {
                "username": channel_username,
                "owner": user_id,
                "required": buying["members"],
                "current": 0,
                "completed": False,
                "reuse_count": 0,
                "joined_users": [],
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "bot_is_admin": True,
                "last_admin_check": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "transaction_id": transaction_id
            }

            save_data(data)
            
            update_system_stats("total_purchases", increment=1)

            await update.message.reply_text(
                f"✅ تم إنشاء الطلب بنجاح!\n\n"
                f"📢 القناة: @{channel_username}\n"
                f"👥 العدد المطلوب: {buying['members']}\n"
                f"💰 المدفوع: {buying['points']} نقطة\n"
                f"⭐ رصيدك الآن: {user_data['points'] - buying['points']}\n"
                f"🆔 رقم الطلب: {order_id}\n\n"
                f"🚀 بدأ التجميع!",
                parse_mode="HTML"
            )

        if "transaction_id" in buying:
            cooldown_manager.mark_transaction_complete(buying["transaction_id"])
    
    context.user_data.pop("buying", None)

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """معالجة الأخطاء العامة"""
    logger.error(f"❌ حدث خطأ: {context.error}")
    
    if update and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "❌ حدث خطأ غير متوقع. تم تسجيله للمطور."
            )
        except:
            pass

# ===================== المهام المجدولة =====================

async def periodic_subscription_check(context: ContextTypes.DEFAULT_TYPE):
    """فحص دوري للاشتراكات - نسخة محسنة مع نظام العداد الذكي"""
    try:
        if not context or not context.bot:
            logger.error("❌ context أو bot غير متوفرين في periodic_subscription_check")
            return
        
        bot = context.bot
        data = load_data(force_reload=True)
        users_data = load_users(force_reload=True)
        
        penalty_count = 0
        counter_decreased = 0
        checked_count = 0
        skipped_count = 0

        logger.info(f"🔍 بدء فحص دوري للاشتراكات: {len(users_data)} مستخدم")

        for user_id, user_data in users_data.items():
            active_channels = user_data.get("active_subscriptions", [])

            if not active_channels:
                continue

            for channel_id in active_channels[:]:  # استخدام نسخة للكشف
                channel = data.get("channels", {}).get(channel_id)
                if not channel:
                    # قناة غير موجودة - إزالتها من النشطة
                    active_subscriptions = user_data.get("active_subscriptions", [])
                    user_data["active_subscriptions"] = [c for c in active_subscriptions if c != channel_id]
                    update_user_data(user_id, {"active_subscriptions": user_data["active_subscriptions"]}, "remove_nonexistent_channel")
                    skipped_count += 1
                    logger.debug(f"🗑️ قناة غير موجودة: {channel_id} للمستخدم {user_id}")
                    continue

                channel_username = channel.get("username", "")
                if not channel_username:
                    skipped_count += 1
                    logger.debug(f"⏭️ قناة بدون يوزر: {channel_id}")
                    continue

                try:
                    checked_count += 1
                    
                    logger.debug(f"🔍 فحص المستخدم {user_id} في القناة @{channel_username}")
                    
                    is_subscribed = await check_channel_subscription(bot, int(user_id), channel_username)
                    
                    if is_subscribed is None:
                        # خطأ في التحقق - تخطي
                        logger.warning(f"⚠️ خطأ في التحقق للمستخدم {user_id} في @{channel_username}")
                        continue
                    
                    # 🔴 المستخدم غادر القناة
                    if is_subscribed is False:
                        logger.info(f"🚨 المستخدم {user_id} غادر القناة @{channel_username}")
                        
                        # التحقق من حالة القناة
                        if channel.get("completed", False):
                            # ⭐ قناة مكتملة - معاملة خاصة بدون خصم
                            mark_channel_as_left(user_id, channel_id, channel)
                            
                            try:
                                await bot.send_message(
                                    int(user_id),
                                    f"📢 القناة: @{channel_username}\n"
                                    f"✅ كانت مكتملة بالفعل\n"
                                    f"👋 تمت إزالتها من قائمتك النشطة\n\n"
                                    f"💡 عندما تعاد إضافة القناة، ستظهر لك مرة أخرى",
                                    parse_mode="HTML"
                                )
                            except Exception as msg_error:
                                logger.error(f"❌ خطأ في إرسال رسالة القناة المكتملة: {msg_error}")
                            
                        else:
                            # 🔴 قناة قيد التجميع - خصم نقاط وتقليل العداد
                            transaction_id = f"penalty_{user_id}_{channel_id}_{int(time.time() * 1000)}"
                            penalty_amount = 5
                            
                            # الحصول على النقاط الحالية
                            current_user_data = get_user_data(user_id, force_reload=True)
                            current_points = current_user_data.get("points", 0)
                            
                            logger.info(f"💸 محاولة خصم {penalty_amount} نقطة من {user_id} (النقاط الحالية: {current_points})")
                            
                            # ✅ 1. تقليل العداد أولاً
                            counter_success, new_counter, counter_msg = await decrease_channel_counter(
                                bot, user_id, channel_id, channel, penalty_amount
                            )
                            
                            if counter_success:
                                counter_decreased += 1
                                logger.info(f"📉 تم تقليل عداد القناة {channel_username}: {counter_msg}")
                            else:
                                logger.error(f"❌ فشل تقليل العداد للقناة {channel_username}: {counter_msg}")
                            
                            # ✅ 2. خصم النقاط
                            success, message = safe_add_points(
                                user_id, 
                                penalty_amount, 
                                "subtract", 
                                "subscription_check_penalty",
                                transaction_id
                            )
                            
                            if success:
                                logger.info(f"✅ تم خصم {penalty_amount} نقطة من المستخدم {user_id}")
                                penalty_count += 1
                            else:
                                logger.error(f"❌ فشل خصم النقاط من المستخدم {user_id}: {message}")
                            
                            # ✅ 3. وضع علامة المغادرة
                            mark_success = mark_channel_as_left(user_id, channel_id, channel)
                            
                            if not mark_success:
                                logger.error(f"❌ فشل وضع علامة المغادرة للمستخدم {user_id} من القناة {channel_id}")
                            
                            # الحصول على البيانات المحدثة
                            updated_user_data = get_user_data(user_id, force_reload=True)
                            final_points = updated_user_data.get("points", 0)
                            
                            logger.info(f"💰 نقاط المستخدم {user_id} بعد الخصم: {final_points}")
                            
                            # الحصول على العداد الجديد
                            updated_channel_data = load_data(force_reload=True).get("channels", {}).get(channel_id, {})
                            final_counter = updated_channel_data.get("current", 0)
                            required_counter = updated_channel_data.get("required", 0)
                            
                            # ✅ 4. إرسال رسالة للمستخدم
                            try:
                                penalty_msg = (
                                    f"⚠️ تحذير: تم خصم نقاط!\n\n"
                                    f"📢 القناة: @{channel_username}\n"
                                    f"💸 السبب: خرجت من القناة قيد التجميع\n"
                                    f"💰 تم خصم: {penalty_amount} نقاط\n"
                                    f"🎯 نقاطك الآن: {final_points}\n"
                                    f"📉 العداد تغيّر: {final_counter}/{required_counter}\n\n"
                                    f"🔄 يمكنك الانضمام مرة أخرى لزيادة العداد\n"
                                    f"💰 ستحصل على 3 نقاط عند عودتك"
                                )
                                
                                await bot.send_message(
                                    int(user_id),
                                    penalty_msg,
                                    parse_mode="HTML"
                                )
                                logger.info(f"📤 تم إرسال رسالة خصم للمستخدم {user_id}")
                                
                            except Exception as send_error:
                                logger.error(f"❌ خطأ في إرسال رسالة الخصم للمستخدم {user_id}: {send_error}")
                            
                            # ✅ 5. إشعار صاحب القناة
                            channel_owner = channel.get("owner")
                            if channel_owner and channel_owner != str(ADMIN_ID):
                                try:
                                    owner_user_data = get_user_data(user_id)
                                    await bot.send_message(
                                        int(channel_owner),
                                        f"⚠️ مغادرة من قناتك!\n\n"
                                        f"📢 القناة: @{channel_username}\n"
                                        f"👤 المستخدم: @{owner_user_data.get('username', user_id)}\n"
                                        f"📉 العداد الآن: {final_counter}/{required_counter}\n"
                                        f"📅 الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                                        parse_mode="HTML"
                                    )
                                    logger.info(f"📤 تم إرسال إشعار لمالك القناة {channel_owner}")
                                except Exception as owner_notify_error:
                                    logger.error(f"❌ خطأ في إشعار صاحب القناة: {owner_notify_error}")
                                
                except Exception as check_error:
                    logger.error(f"❌ خطأ في فحص الاشتراك للمستخدم {user_id} في القناة {channel_id}: {check_error}")
                    import traceback
                    traceback.print_exc()
        
        # تسجيل الإحصائيات
        logger.info(
            f"📊 نتائج الفحص الدوري الذكي:\n"
            f"  ✅ تم فحص: {checked_count} اشتراك\n"
            f"  💸 تم خصم نقاط من: {penalty_count} مستخدم\n"
            f"  📉 تم تقليل العداد لـ: {counter_decreased} قناة\n"
            f"  ⏭️ تم تجاهل: {skipped_count} قناة"
        )
        
        # تسجيل مفصل للمساعدة في التصحيح
        logger.debug(f"📋 بيانات الإحصاءات النهائية:")
        logger.debug(f"  - penalty_count: {penalty_count}")
        logger.debug(f"  - counter_decreased: {counter_decreased}")
        logger.debug(f"  - checked_count: {checked_count}")
        
    except Exception as e:
        logger.error(f"❌ خطأ كبير في فحص الاشتراكات الدوري: {e}")
        import traceback
        traceback.print_exc()

async def test_penalty(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """اختبار خصم النقاط مباشرة"""
    user_id = str(update.message.from_user.id)
    
    if not is_admin(update.message.from_user.id):
        await update.message.reply_text("❌ هذا الأمر للمطور فقط!")
        return
    
    penalty_amount = 5
    transaction_id = f"test_penalty_{user_id}_{int(time.time() * 1000)}"
    
    # خصم مباشر
    success, message = safe_add_points(
        user_id, 
        penalty_amount, 
        "subtract", 
        "test_penalty",
        transaction_id
    )
    
    if success:
        user_data = get_user_data(user_id)
        await update.message.reply_text(
            f"✅ اختبار الخصم ناجح!\n\n"
            f"💰 تم خصم: {penalty_amount} نقطة\n"
            f"🎯 نقاطك الآن: {user_data['points']}\n"
            f"🆔 معرّف المعاملة: {transaction_id}",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            f"❌ اختبار الخصم فاشل!\n\n"
            f"📝 الرسالة: {message}\n"
            f"🆔 معرّف المعاملة: {transaction_id}",
            parse_mode="HTML"
        )

async def storage_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض معلومات التخزين"""
    user_id = str(update.message.from_user.id)
    
    if not is_admin(int(user_id)):
        await update.message.reply_text("❌ هذا الأمر للمشرفين فقط!")
        return
    
    try:
        # معلومات الملفات
        users_size = os.path.getsize(USERS_FILE) if os.path.exists(USERS_FILE) else 0
        data_size = os.path.getsize(DATA_FILE) if os.path.exists(DATA_FILE) else 0
        
        # عدد النسخ الاحتياطية
        backup_count = 0
        backup_total_size = 0
        if os.path.exists(BACKUP_DIR):
            for file in os.listdir(BACKUP_DIR):
                if file.endswith('.bak'):
                    file_path = os.path.join(BACKUP_DIR, file)
                    backup_total_size += os.path.getsize(file_path)
                    backup_count += 1
        
        # تحويل الحجم
        def format_size(bytes_size):
            for unit in ['B', 'KB', 'MB']:
                if bytes_size < 1024:
                    return f"{bytes_size:.2f} {unit}"
                bytes_size /= 1024
            return f"{bytes_size:.2f} GB"
        
        # تحميل البيانات للإحصائيات
        users_data = load_users()
        data_info = load_data()
        
        message = (
            f"📊 **معلومات التخزين المحلي**\n\n"
            f"📁 **المسارات:**\n"
            f"• المجلد الرئيسي: `{BOT_DIR}`\n"
            f"• مجلد النسخ: `{BACKUP_DIR}`\n\n"
            
            f"📄 **الملفات الرئيسية:**\n"
            f"• `users.json`: {format_size(users_size)} ({len(users_data)} مستخدم)\n"
            f"• `data.json`: {format_size(data_size)}\n"
            f"• القنوات: {len(data_info.get('channels', {}))}\n\n"
            
            f"💾 **النسخ الاحتياطية:**\n"
            f"• العدد: {backup_count} نسخة\n"
            f"• الحجم الإجمالي: {format_size(backup_total_size)}\n\n"
            
            f"📅 **آخر تحديث:**\n"
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        # زر لعرض محتويات المجلد
        keyboard = [
            [InlineKeyboardButton("🔄 تحديث المعلومات", callback_data="refresh_storage_info")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel")]
        ]
        
        await update.message.reply_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"❌ خطأ في storage_info: {e}")
        await update.message.reply_text(f"❌ خطأ: {str(e)[:100]}")

def fix_left_completed_flags():
    """إصلاح علامات left_completed القديمة مع الجولات الجديدة"""
    try:
        users_data = load_users()
        data = load_data()
        fixed_count = 0
        
        for user_id, user_data in users_data.items():
            if "joined_channels" not in user_data:
                continue
                
            for channel_id, join_info in user_data["joined_channels"].items():
                if join_info.get("left_completed", False):
                    # الحصول على بيانات القناة الحالية
                    channel_data = data.get("channels", {}).get(channel_id)
                    if not channel_data:
                        continue
                    
                    completed_round = join_info.get("completed_round", -1)
                    current_round = channel_data.get("reuse_count", 0)
                    
                    # 🔥 🔥 🔥 **هنا الخطأ!** 🔥 🔥 🔥
                    # completed_round أو current_round قد يكون None
                    # نحتاج للتحقق من النوع أولاً
                    
                    completed_round_val = completed_round if completed_round is not None else -1
                    current_round_val = current_round if current_round is not None else 0
                    
                    # إذا كانت هناك جولة جديدة
                    if current_round_val > completed_round_val:
                        # إزالة العلامة القديمة
                        join_info["left_completed"] = False
                        if "completed_round" in join_info:
                            del join_info["completed_round"]
                        if "completed_at" in join_info:
                            del join_info["completed_at"]
                        
                        # إضافة إلى temp_left_channels ليرى القناة
                        temp_left = user_data.get("temp_left_channels", [])
                        if channel_id not in temp_left:
                            temp_left.append(channel_id)
                            user_data["temp_left_channels"] = temp_left
                        
                        fixed_count += 1
        
        if fixed_count > 0:
            save_users(users_data)
            logger.info(f"🔧 تم إصلاح {fixed_count} علامة left_completed قديمة")
            
        return fixed_count
        
    except Exception as e:
        logger.error(f"❌ خطأ في fix_left_completed_flags: {e}")
        import traceback
        traceback.print_exc()
        return 0

async def periodic_cleanup(context: ContextTypes.DEFAULT_TYPE):
    """تنظيف دوري للبيانات"""
    try:
        # تنظيف المستخدمين غير النشطين (أكثر من 30 يوم)
        users_data = load_users()
        month_ago = datetime.now() - timedelta(days=30)
        inactive_count = 0
        
        for user_id, user_data in list(users_data.items()):
            last_active_str = user_data.get("last_active")
            if last_active_str:
                try:
                    last_active = datetime.strptime(last_active_str, "%Y-%m-%d %H:%M:%S")
                    if last_active < month_ago:
                        user_data["inactive"] = True
                        update_user_data(user_id, {"inactive": True}, "inactive_mark")
                        inactive_count += 1
                except:
                    pass
        
        if inactive_count > 0:
            logger.info(f"🧹 تم وضع علامة على {inactive_count} مستخدم كمقصر")
        
        # تنظيف الكتم المنتهي
        cleanup_expired_mutes()
        
    except Exception as e:
        logger.error(f"خطأ في التنظيف الدوري: {e}")

async def auto_completion_check(context: ContextTypes.DEFAULT_TYPE):
    """فحص تلقائي لاكتمال القنوات"""
    try:
        completed_count = check_and_mark_completed_channels()
        
        if completed_count > 0:
            logger.info(f"✅ تم التحقق من {completed_count} قناة مكتملة")
            
    except Exception as e:
        logger.error(f"خطأ في auto_completion_check: {e}")

def cleanup_old_transactions(context: ContextTypes.DEFAULT_TYPE = None):
    """تنظيف المعاملات القديمة"""
    cooldown_manager.clear_old_transactions()

def fix_channel_data_consistency(context: ContextTypes.DEFAULT_TYPE = None):
    """تصحيح تناسق بيانات القنوات"""
    try:
        users_data = load_users()
        data = load_data()
        channels = data.get("channels", {})
        
        for user_id, user_data in users_data.items():
            # التأكد من تناسق القنوات النشطة مع joined_channels
            active_subs = user_data.get("active_subscriptions", [])
            joined_channels = user_data.get("joined_channels", {})
            
            # إزالة القنوات غير الموجودة من النشطة
            valid_active = []
            for channel_id in active_subs:
                if (channel_id in joined_channels and 
                    joined_channels[channel_id].get("verified", False) and
                    not joined_channels[channel_id].get("left", False) and
                    channel_id in channels and not channels[channel_id].get("completed", False)):
                    valid_active.append(channel_id)
            
            if len(valid_active) != len(active_subs):
                updates = {"active_subscriptions": valid_active}
                update_user_data(user_id, updates, "data_consistency_fix")
        
    except Exception as e:
        logger.error(f"خطأ في تصحيح بيانات القنوات: {e}")

def repair_corrupted_data():
    """إصلاح البيانات التالفة"""
    repaired = False
    
    # إصلاح ملف المستخدمين
    if os.path.exists(USERS_FILE):
        try:
            with open(USERS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            for user_id, user_data in list(data.items()):
                if not isinstance(user_data, dict):
                    del data[user_id]
                    repaired = True
                    continue
                
                if not isinstance(user_data.get("points", 0), (int, float)):
                    user_data["points"] = 0
                    repaired = True
                
                if not isinstance(user_data.get("invites", 0), int):
                    user_data["invites"] = 0
                    repaired = True
            
            if repaired:
                save_users(data, backup=False)
                
        except Exception:
            pass
    
    return repaired

def update_user_channel_join_info(user_id, channel_id, channel_username, current_round, reactivated_at, points_earned, transaction_id):
    """تحديث معلومات انضمام المستخدم للقناة مع جميع التفاصيل"""
    
    user_data = get_user_data(user_id, force_reload=True)
    
    # 1. تحديث joined_channels
    joined_channels = user_data.get("joined_channels", {})
    
    join_info = {
        "channel_username": channel_username,
        "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "verified": True,
        "points_earned": points_earned,
        "left": False,
        "round": current_round,  # جولة القناة الحالية
        "reactivated_at": reactivated_at,  # تاريخ إعادة تفعيل القناة (إذا وجد)
        "channel_reactivated": bool(reactivated_at),  # هل تمت إعادة التفعيل؟
        "join_round": current_round + 1,  # رقم الجولة عند الانضمام
        "verified_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "transaction_id": transaction_id,
        "last_verified": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "status": "active",
        "join_type": "new" if not reactivated_at else "reactivated"
    }
    
    # 2. حفظ معلومات سابقة إذا كانت هناك انضمامات قديمة
    if channel_id in joined_channels:
        old_info = joined_channels[channel_id]
        
        # حفظ السجل القديم في previous_versions
        if "previous_versions" not in join_info:
            join_info["previous_versions"] = []
        
        join_info["previous_versions"].append({
            "old_round": old_info.get("round", 0),
            "old_joined_at": old_info.get("joined_at"),
            "old_reactivated_at": old_info.get("reactivated_at"),
            "old_points_earned": old_info.get("points_earned", 0),
            "archived_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
    
    # 3. تحديث joined_channels
    joined_channels[channel_id] = join_info
    
    # 4. تحديث active_subscriptions
    active_subscriptions = user_data.get("active_subscriptions", [])
    if channel_id not in active_subscriptions:
        active_subscriptions.append(channel_id)
    
    # 5. إزالة من القنوات المتروكة مؤقتاً إذا كان فيها
    temp_left_channels = user_data.get("temp_left_channels", [])
    if channel_id in temp_left_channels:
        temp_left_channels.remove(channel_id)
    
    # 6. إزالة من القنوات المتروكة نهائياً إذا كان فيها
    permanent_left_channels = user_data.get("permanent_left_channels", [])
    if channel_id in permanent_left_channels:
        permanent_left_channels.remove(channel_id)
    
    # 7. إزالة من left_channels القديم إذا كان فيها
    left_channels = user_data.get("left_channels", [])
    if channel_id in left_channels:
        left_channels.remove(channel_id)
    
    # 8. تسجيل في السجل التاريخي
    if "join_history" not in user_data:
        user_data["join_history"] = []
    
    user_data["join_history"].append({
        "channel_id": channel_id,
        "channel_username": channel_username,
        "joined_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "round": current_round,
        "reactivated_at": reactivated_at,
        "points_earned": points_earned,
        "transaction_id": transaction_id,
        "type": "new_join"
    })
    
    # 9. إعداد التحديثات الكاملة
    updates = {
        "joined_channels": joined_channels,
        "active_subscriptions": active_subscriptions,
        "temp_left_channels": temp_left_channels,
        "permanent_left_channels": permanent_left_channels,
        "left_channels": left_channels,
        "join_history": user_data.get("join_history", []),
        "last_active": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "inactive": False
    }
    
    # 10. حفظ التحديثات
    success = update_user_data(
        user_id, 
        updates, 
        "channel_join_update", 
        transaction_id
    )
    
    if success:
        logger.info(f"✅ تم تحديث معلومات انضمام {user_id} للقناة {channel_username} - الجولة {current_round}")
        return True, join_info
    else:
        logger.error(f"❌ فشل تحديث معلومات انضمام {user_id} للقناة {channel_username}")
        return False, None

def create_backup():
    """إنشاء نسخة احتياطية محسنة في المسار المحلي"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    backup_files = []
    for filename in [USERS_FILE, DATA_FILE]:
        if os.path.exists(filename):
            backup_name = os.path.join(BACKUP_DIR, f"{os.path.basename(filename)}.{timestamp}.bak")
            try:
                shutil.copy2(filename, backup_name)
                backup_files.append(backup_name)
                logger.info(f"📦 نسخة احتياطية: {backup_name}")
            except Exception as e:
                logger.error(f"❌ خطأ في نسخ {filename}: {e}")
    
    # حذف النسخ القديمة (احتفظ بـ 10 نسخ فقط)
    try:
        if os.path.exists(BACKUP_DIR):
            backup_files_list = sorted([f for f in os.listdir(BACKUP_DIR) if f.endswith(".bak")])
            # حذف كل الملفات القديمة باستثناء آخر 10
            for old_backup in backup_files_list[:-10]:
                old_path = os.path.join(BACKUP_DIR, old_backup)
                try:
                    os.remove(old_path)
                    logger.debug(f"🧹 حذف نسخة قديمة: {old_backup}")
                except Exception as e:
                    logger.error(f"❌ خطأ في حذف {old_backup}: {e}")
    except Exception as e:
        logger.error(f"❌ خطأ في حذف النسخ القديمة: {e}")
    
    return backup_files
    
# ===================== نظام النسخ الاحتياطي =====================

BACKUP_INTERVAL = 1800  # كل 60 ثانية (دقيقة واحدة)
LAST_BACKUP_TIME = 0

def auto_backup_manager():
    """مدير النسخ الاحتياطي التلقائي"""
    global LAST_BACKUP_TIME
    
    logger.info("🔄 بدء مدير النسخ الاحتياطي التلقائي")
    
    while True:
        try:
            current_time = time.time()
            
            if current_time - LAST_BACKUP_TIME >= BACKUP_INTERVAL:
                LAST_BACKUP_TIME = current_time
                create_local_backup()
                
                logger.debug(f"✅ نسخة احتياطية محلية: {datetime.now().strftime('%H:%M:%S')}")
            
            time.sleep(10)  # فحص كل 10 ثواني
            
        except Exception as e:
            logger.error(f"❌ خطأ في مدير النسخ الاحتياطي: {e}")
            time.sleep(30)

def create_local_backup():
    """إنشاء نسخة احتياطية محلية"""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for filename in [USERS_FILE, DATA_FILE]:
            if os.path.exists(filename):
                backup_name = os.path.join(BACKUP_DIR, f"{os.path.basename(filename)}.{timestamp}.bak")
                try:
                    shutil.copy2(filename, backup_name)
                except Exception as e:
                    logger.error(f"❌ فشل نسخ {filename}: {e}")
        
        # حذف النسخ القديمة (احتفظ بـ 5 نسخ فقط)
        cleanup_old_backups()
        
    except Exception as e:
        logger.error(f"❌ خطأ في إنشاء النسخة المحلية: {e}")

def cleanup_old_backups():
    """تنظيف النسخ الاحتياطية القديمة"""
    try:
        if not os.path.exists(BACKUP_DIR):
            return
        
        # جمع جميع ملفات الباك أب
        backup_files = []
        for file in os.listdir(BACKUP_DIR):
            if file.endswith('.bak'):
                file_path = os.path.join(BACKUP_DIR, file)
                backup_files.append((file_path, os.path.getctime(file_path)))
        
        # ترتيب من الأقدم للأحدث
        backup_files.sort(key=lambda x: x[1])
        
        # حذف الزائدة عن 5 نسخ
        if len(backup_files) > 5:
            files_to_delete = backup_files[:-5]  # احتفظ بآخر 5 نسخ
            for file_path, _ in files_to_delete:
                try:
                    os.remove(file_path)
                    logger.debug(f"🗑️ تم حذف نسخة قديمة: {os.path.basename(file_path)}")
                except Exception as e:
                    logger.error(f"❌ فشل حذف {file_path}: {e}")
                    
    except Exception as e:
        logger.error(f"❌ خطأ في تنظيف النسخ القديمة: {e}")

async def send_backup_to_owner(context: ContextTypes.DEFAULT_TYPE):
    """إرسال النسخة الاحتياطية للمالك (تلقائي كل دقيقة)"""
    try:
        bot = context.bot
        
        # التحقق من وجود الملفات
        if not os.path.exists(USERS_FILE) or not os.path.exists(DATA_FILE):
            return
        
        # إرسال users.json
        try:
            with open(USERS_FILE, 'rb') as f:
                await bot.send_document(
                    chat_id=ADMIN_ID,
                    document=f,
                    filename=f"users_{datetime.now().strftime('%H%M%S')}.json",
                    caption=f"📁 users.json | {datetime.now().strftime('%H:%M:%S')}"
                )
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال users.json: {e}")
        
        # إرسال data.json
        try:
            with open(DATA_FILE, 'rb') as f:
                await bot.send_document(
                    chat_id=ADMIN_ID,
                    document=f,
                    filename=f"data_{datetime.now().strftime('%H%M%S')}.json",
                    caption=f"📁 data.json | {datetime.now().strftime('%H:%M:%S')}"
                )
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال data.json: {e}")
        
        logger.debug(f"📤 تم إرسال نسخة للمالك: {datetime.now().strftime('%H:%M:%S')}")
        
    except Exception as e:
        logger.error(f"❌ خطأ عام في إرسال الباك أب: {e}")

async def get_backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """طلب نسخة احتياطية يدوياً"""
    user_id = str(update.message.from_user.id)
    
    if user_id != str(ADMIN_ID):
        await update.message.reply_text("❌ هذا الأمر للمالك فقط!")
        return
    
    await update.message.reply_text("📤 جاري إرسال النسخة الاحتياطية...")
    
    # إنشاء نسخة محلية أولاً
    create_local_backup()
    
    # إرسال الملفات
    bot = context.bot
    try:
        # إرسال users.json
        if os.path.exists(USERS_FILE):
            with open(USERS_FILE, 'rb') as f:
                await bot.send_document(
                    chat_id=ADMIN_ID,
                    document=f,
                    filename="users_latest.json",
                    caption="📁 users.json (يدوياً)"
                )
        
        # إرسال data.json
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, 'rb') as f:
                await bot.send_document(
                    chat_id=ADMIN_ID,
                    document=f,
                    filename="data_latest.json",
                    caption="📁 data.json (يدوياً)"
                )
        
        await update.message.reply_text("✅ تم إرسال النسخة الاحتياطية!")
        
    except Exception as e:
        logger.error(f"❌ خطأ في الأمر getbackup: {e}")
        await update.message.reply_text("❌ حدث خطأ في إرسال النسخة!")

async def send_backup_files_to_owner(bot):
    """إرسال ملفات JSON للمالك كل دقيقة"""
    global _last_backup_time
    
    current_time = time.time()
    
    # التحقق من الوقت
    if current_time - _last_backup_time < BACKUP_INTERVAL:
        return
    
    _last_backup_time = current_time
    
    try:
        # التحقق من وجود الملفات
        if not os.path.exists(USERS_FILE) or not os.path.exists(DATA_FILE):
            logger.warning("❌ ملفات البيانات غير موجودة للإرسال")
            return
        
        # إرسال ملف المستخدمين
        try:
            with open(USERS_FILE, 'rb') as users_file:
                await bot.send_document(
                    chat_id=ADMIN_ID,
                    document=users_file,
                    filename="users.json",
                    caption=f"📁 users.json\n⏰ {datetime.now().strftime('%H:%M:%S')}"
                )
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال users.json: {e}")
        
        # إرسال ملف البيانات
        try:
            with open(DATA_FILE, 'rb') as data_file:
                await bot.send_document(
                    chat_id=ADMIN_ID,
                    document=data_file,
                    filename="data.json",
                    caption=f"📁 data.json\n⏰ {datetime.now().strftime('%H:%M:%S')}"
                )
        except Exception as e:
            logger.error(f"❌ خطأ في إرسال data.json: {e}")
        
        logger.info(f"✅ تم إرسال نسخة احتياطية للمالك: {datetime.now().strftime('%H:%M:%S')}")
        
    except Exception as e:
        logger.error(f"❌ خطأ عام في إرسال الباك أب: {e}")

async def storage_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """عرض معلومات التخزين"""
    user_id = str(update.message.from_user.id)
    
    if not is_admin(int(user_id)):
        await update.message.reply_text("❌ هذا الأمر للمشرفين فقط!")
        return
    
    try:
        # معلومات الملفات
        users_size = os.path.getsize(USERS_FILE) if os.path.exists(USERS_FILE) else 0
        data_size = os.path.getsize(DATA_FILE) if os.path.exists(DATA_FILE) else 0
        
        # عدد النسخ الاحتياطية
        backup_count = 0
        backup_total_size = 0
        if os.path.exists(BACKUP_DIR):
            for file in os.listdir(BACKUP_DIR):
                if file.endswith('.bak'):
                    file_path = os.path.join(BACKUP_DIR, file)
                    backup_total_size += os.path.getsize(file_path)
                    backup_count += 1
        
        # تحويل الحجم
        def format_size(bytes_size):
            for unit in ['B', 'KB', 'MB']:
                if bytes_size < 1024:
                    return f"{bytes_size:.2f} {unit}"
                bytes_size /= 1024
            return f"{bytes_size:.2f} GB"
        
        # تحميل البيانات للإحصائيات
        users_data = load_users()
        data_info = load_data()
        
        message = (
            f"📊 **معلومات التخزين المحلي**\n\n"
            f"📁 **المسارات:**\n"
            f"• المجلد الرئيسي: `{BOT_DIR}`\n"
            f"• مجلد النسخ: `{BACKUP_DIR}`\n\n"
            
            f"📄 **الملفات الرئيسية:**\n"
            f"• `users.json`: {format_size(users_size)} ({len(users_data)} مستخدم)\n"
            f"• `data.json`: {format_size(data_size)}\n"
            f"• القنوات: {len(data_info.get('channels', {}))}\n\n"
            
            f"💾 **النسخ الاحتياطية:**\n"
            f"• العدد: {backup_count} نسخة\n"
            f"• الحجم الإجمالي: {format_size(backup_total_size)}\n\n"
            
            f"📅 **آخر تحديث:**\n"
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        
        # زر لعرض محتويات المجلد
        keyboard = [
            [InlineKeyboardButton("🔄 تحديث المعلومات", callback_data="refresh_storage_info")],
            [InlineKeyboardButton("🔙 رجوع", callback_data="admin_panel")]
        ]
        
        await update.message.reply_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        
    except Exception as e:
        logger.error(f"❌ خطأ في storage_info: {e}")
        await update.message.reply_text(f"❌ خطأ: {str(e)[:100]}")
# ===================== الدالة الرئيسية =====================

def main():
    """الدالة الرئيسية"""
    logger.info("🚀 بدء تشغيل البوت...")
    
    try:
        # ========== التحقق من المسارات المحلية ==========
        logger.info(f"📁 التحقق من المسار: {BOT_DIR}")
        
        # إنشاء المجلدات الضرورية على الهاتف
        required_folders = [BOT_DIR, BACKUP_DIR]
        for folder in required_folders:
            if not os.path.exists(folder):
                try:
                    os.makedirs(folder, exist_ok=True)
                    logger.info(f"✅ تم إنشاء مجلد: {folder}")
                except Exception as e:
                    logger.error(f"❌ فشل إنشاء مجلد {folder}: {e}")
                    print(f"\n❌ خطأ: لا يمكن إنشاء مجلد {folder}")
                    print("💡 تأكد من:")
                    print(f"1. صلاحيات الكتابة في: /storage/emulated/0/")
                    print("2. مساحة تخزين كافية")
                    print("3. أن الهاتف غير مقفل")
                    return
        
        # 🔧 التحقق من صلاحيات الكتابة
        try:
            # اختبار الكتابة
            test_file = os.path.join(BOT_DIR, "test_write.txt")
            with open(test_file, 'w') as f:
                f.write("test")
            os.remove(test_file)
            logger.info("✅ صلاحيات الكتابة صالحة")
        except Exception as e:
            logger.error(f"❌ لا توجد صلاحيات كتابة في {BOT_DIR}: {e}")
            print(f"\n❌ خطأ: لا يمكن الكتابة في {BOT_DIR}")
            print("💡 الحلول:")
            print("1. استخدم تطبيق Termux")
            print("2. استخدم مسار /data/data/com.termux/files/home/")
            print("3. تأكد من إذن التخزين")
            return
        
        # 🔧 التحقق من وجود ملفات البيانات وإنشاؤها إذا لزم
        logger.info("🔍 فحص ملفات البيانات...")
        
        if not os.path.exists(DATA_FILE):
            logger.info(f"📝 إنشاء ملف بيانات جديد: {DATA_FILE}")
            try:
                save_data(create_initial_data())
                logger.info(f"✅ تم إنشاء ملف البيانات: {DATA_FILE}")
            except Exception as e:
                logger.error(f"❌ فشل إنشاء {DATA_FILE}: {e}")
                return
        
        if not os.path.exists(USERS_FILE):
            logger.info(f"📝 إنشاء ملف مستخدمين جديد: {USERS_FILE}")
            try:
                save_users({})
                logger.info(f"✅ تم إنشاء ملف المستخدمين: {USERS_FILE}")
            except Exception as e:
                logger.error(f"❌ فشل إنشاء {USERS_FILE}: {e}")
                return
        
        # 🔧 تحميل البيانات للتحقق
        try:
            data = load_data()
            users_data = load_users()
            logger.info(f"📊 تم تحميل {len(users_data)} مستخدم و {len(data.get('channels', {}))} قناة")
            
            # عرض معلومات الملفات
            users_size = os.path.getsize(USERS_FILE) if os.path.exists(USERS_FILE) else 0
            data_size = os.path.getsize(DATA_FILE) if os.path.exists(DATA_FILE) else 0
            logger.info(f"💾 حجم الملفات: users.json={users_size:,} bytes, data.json={data_size:,} bytes")
            
        except Exception as e:
            logger.error(f"⚠️ خطأ في تحميل البيانات، سيتم استخدام البيانات الافتراضية: {e}")
            # استخدام البيانات الافتراضية
            data = create_initial_data()
            users_data = {}
            logger.warning("⚠️ استخدام البيانات الافتراضية")
        
        # 🔧 تنظيف البيانات الأولي (محدود وآمن)
        try:
            # تنظيف القنوات المكتملة فقط
            completed_count = check_and_mark_completed_channels()
            if completed_count > 0:
                logger.info(f"🧹 تم تنظيف {completed_count} قناة مكتملة")
        except Exception as e:
            logger.error(f"⚠️ خطأ في تنظيف القنوات المكتملة: {e}")
        
        # ========== بدء النسخ الاحتياطي التلقائي ==========
        try:
            backup_thread = threading.Thread(target=auto_backup_manager, daemon=True)
            backup_thread.start()
            logger.info("🔄 تم تشغيل مدير النسخ الاحتياطي التلقائي")
        except Exception as e:
            logger.error(f"⚠️ فشل تشغيل النسخ الاحتياطي التلقائي: {e}")
        
        # ========== إنشاء التطبيق ==========
        logger.info("🤖 إنشاء تطبيق البوت...")
        application = Application.builder().token(TOKEN).build()
        
        # ========== إضافة الـ handlers الأساسية ==========
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("code", handle_code_command))
        application.add_handler(CommandHandler("test_penalty", test_penalty))
        application.add_handler(CommandHandler("storage", storage_info))
        application.add_handler(CommandHandler("getbackup", get_backup_command))  # ⭐ أمر جديد
        
        # أزرار الكيبورد
        application.add_handler(CallbackQueryHandler(button_handler, pattern=".*"))
        
        # رسائل الأدمن
        try:
            if ADMIN_ID and str(ADMIN_ID).isdigit():
                admin_filter = filters.User(user_id=int(ADMIN_ID))
                application.add_handler(
                    MessageHandler(
                        filters.TEXT & admin_filter & ~filters.COMMAND,
                        handle_admin_message
                    ),
                    group=0
                )
                logger.info("👑 تم إعداد مرشح الأدمن")
            else:
                logger.warning("⚠️ ID الأدمن غير صالح، سيتم تعطيل أوامر الأدمن")
        except Exception as e:
            logger.error(f"❌ خطأ في إعداد مرشح الأدمن: {e}")
        
        # رسائل الشراء (مجموعة 1)
        application.add_handler(
            MessageHandler(
                filters.TEXT & ~filters.COMMAND,
                handle_channel_purchase
            ),
            group=1
        )
        
        # الرسائل العامة (مجموعة 2)
        application.add_handler(
            MessageHandler(
                filters.TEXT & ~filters.COMMAND,
                handle_general_messages
            ),
            group=2
        )
        
        # معالج الأخطاء
        application.add_error_handler(error_handler)
        
        # ========== المهام المجدولة ==========
        logger.info("⏰ جدولة المهام...")
        
        # المهام الأساسية المضمونة العمل
        scheduled_tasks = [
            ("فحص الاشتراكات", periodic_subscription_check, 30, 30),
            ("تنظيف الكتم المنتهي", cleanup_expired_mutes, 3600, 60),
            ("فحص اكتمال القنوات", auto_completion_check, 120, 60),
            ("تنظيف المعاملات القديمة", cleanup_old_transactions, 3600, 120),
            ("إرسال النسخ الاحتياطية", send_backup_to_owner, 1800, 10),  # ⭐ مهمة جديدة
        ]
        
        successful_tasks = 0
        for task_name, task_func, interval, first_delay in scheduled_tasks:
            try:
                application.job_queue.run_repeating(
                    task_func,
                    interval=interval,
                    first=first_delay,
                    name=task_name
                )
                successful_tasks += 1
                logger.info(f"✅ تم جدولة {task_name} (كل {interval} ثانية)")
            except Exception as e:
                logger.error(f"❌ فشل جدولة {task_name}: {e}")
        
        # المهام الاختيارية (إذا نجحت المهام الأساسية)
        if successful_tasks >= 2:  # إذا نجحت على الأقل مهمتين أساسيتين
            optional_tasks = [
                ("تنظيف البيانات", periodic_cleanup, 86400, 600),
                ("تصحيح بيانات القنوات", fix_channel_data_consistency, 1800, 300),
            ]
            
            for task_name, task_func, interval, first_delay in optional_tasks:
                try:
                    application.job_queue.run_repeating(
                        task_func,
                        interval=interval,
                        first=first_delay,
                        name=task_name
                    )
                    logger.info(f"➕ تم جدولة {task_name} (كل {interval} ثانية)")
                except Exception as e:
                    logger.warning(f"⚠️ فشل جدولة المهمة الاختيارية {task_name}: {e}")
        
        # ========== معلومات بدء التشغيل النهائية ==========
        logger.info("=" * 60)
        logger.info("🎉 البوت يعمل الآن بنجاح!")
        logger.info(f"👤 مالك البوت: {ADMIN_ID}")
        logger.info(f"📢 قناة البوت: {BOT_CHANNEL}")
        logger.info(f"📁 المسار الرئيسي: {BOT_DIR}")
        logger.info(f"💾 ملفات البيانات:")
        logger.info(f"   • users.json: {USERS_FILE}")
        logger.info(f"   • data.json: {DATA_FILE}")
        logger.info(f"   • backups: {BACKUP_DIR}")
        logger.info(f"⏰ المهام المجدولة: {successful_tasks}/{len(scheduled_tasks)}")
        logger.info(f"📤 النسخ الاحتياطي: كل {BACKUP_INTERVAL} ثانية")
        logger.info(f"🕒 وقت البدء: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        # ========== تشغيل البوت ==========
        logger.info("🟢 بدء polling...")
        try:
            application.run_polling(
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES,
                poll_interval=1.0,
                timeout=30,
                close_loop=False
            )
        except KeyboardInterrupt:
            logger.info("⏹️ إيقاف البوت بواسطة المستخدم...")
            print("\n" + "=" * 50)
            print("🛑 تم إيقاف البوت بنجاح!")
            print(f"📁 البيانات محفوظة في: {BOT_DIR}")
            print(f"📁 النسخ الاحتياطية: {BACKUP_DIR}")
            print(f"⏰ الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 50)
        except Exception as polling_error:
            logger.error(f"❌ خطأ في polling: {polling_error}")
            raise
        
    except Exception as e:
        logger.error(f"❌ خطأ غير متوقع في main: {e}")
        import traceback
        traceback.print_exc()
        
        # محاولة إنشاء نسخة احتياطية طارئة
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_files = []
            for filename in [USERS_FILE, DATA_FILE]:
                if os.path.exists(filename):
                    backup_name = os.path.join(BACKUP_DIR, f"{os.path.basename(filename)}.crash.{timestamp}.bak")
                    try:
                        shutil.copy2(filename, backup_name)
                        backup_files.append(backup_name)
                        logger.info(f"💾 تم حفظ نسخة احتياطية طارئة: {backup_name}")
                    except Exception as copy_error:
                        logger.error(f"❌ فشل في حفظ النسخة الاحتياطية لـ {filename}: {copy_error}")
            
            if backup_files:
                print(f"\n💾 تم حفظ نسخة احتياطية طارئة في: {backup_files}")
                print(f"📁 المسار: {BACKUP_DIR}")
        except Exception as backup_error:
            logger.error(f"❌ خطأ في النسخ الاحتياطي الطارئ: {backup_error}")
        
        print("\n" + "=" * 60)
        print("❌ حدث خطأ غير متوقع في تشغيل البوت!")
        print(f"📋 الخطأ: {str(e)[:100]}...")
        print(f"📁 مسار البيانات: {BOT_DIR}")
        print(f"⏰ الوقت: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
        # إعطاء خيار للمستخدم
        print("\n💡 نصائح استكشاف الأخطاء:")
        print("1. تحقق من توكن البوت")
        print(f"2. تحقق من صلاحيات المجلد: {BOT_DIR}")
        print("3. جرب تشغيل البوت من تطبيق Termux")
        print("4. تأكد من اتصال الإنترنت")
        print("5. تحقق من مساحة التخزين")
        
        # سؤال المستخدم عما إذا كان يريد إعادة المحاولة
        try:
            retry = input("\nهل تريد إعادة تشغيل البوت؟ (y/n): ").strip().lower()
            if retry == 'y':
                print("🔄 إعادة تشغيل البوت...")
                time.sleep(2)
                main()  # إعادة التشغيل
        except:
            pass


# ===================== تشغيل البوت =====================

if __name__ == "__main__":
    # إضافة معالجة Ctrl+C أنيقة
    import signal
    
    def signal_handler(signum, frame):
        print("\n\n⚠️ تم الضغط على Ctrl+C، جاري الإغلاق...")
        logger.info("⚠️ تم استقبال إشارة الإغلاق (Ctrl+C)")
        raise KeyboardInterrupt
    
    # تسجيل معالج الإشارة
    signal.signal(signal.SIGINT, signal_handler)
    
    # تشغيل البوت مع معالجة الأخطاء
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            main()
            break  # الخروج إذا نجح
        except KeyboardInterrupt:
            print("\n👋 وداعاً!")
            break
        except Exception as e:
            retry_count += 1
            print(f"\n⚠️ المحاولة {retry_count}/{max_retries} فشلت: {str(e)[:50]}...")
            
            if retry_count < max_retries:
                wait_time = retry_count * 5  # زيادة وقت الانتظار مع كل محاولة
                print(f"⏳ إعادة المحاولة بعد {wait_time} ثانية...")
                time.sleep(wait_time)
            else:
                print(f"❌ فشلت جميع المحاولات ({max_retries})")
                print("🔧 يرجى التحقق من:")
                print("  1. توكن البوت")
                print("  2. اتصال الإنترنت")
                print("  3. صلاحيات الملفات")
                print("  4. ملفات البيانات (جرب حذف data.json و users.json)")