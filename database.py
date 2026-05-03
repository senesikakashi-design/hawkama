"""
Database Management Module - v4.0 with Permissions & Notifications
"""

import sqlite3
import hashlib
import os
import shutil
from datetime import datetime
from typing import Dict, List, Optional

class Database:
    """إدارة قاعدة البيانات"""

    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = os.environ.get(
                "DATABASE_PATH", 
                "workflow_v4.db"
            )

        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self.db_path = db_path
        self.init_database()
        self._migrate_database()

    def get_connection(self):
        """إنشاء اتصال بقاعدة البيانات"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def init_database(self):
        """تهيئة قاعدة البيانات"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # جدول المستخدمين
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                full_name TEXT NOT NULL,
                email TEXT,
                role TEXT NOT NULL,
                department TEXT NOT NULL,
                branch_id INTEGER,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (branch_id) REFERENCES branches(id)
            )
        """)

        # جدول الفروع
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS branches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                code TEXT UNIQUE NOT NULL,
                location TEXT,
                manager_name TEXT,
                contact_phone TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # جدول الطلبات
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS service_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_type TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                priority TEXT DEFAULT 'medium',
                status TEXT DEFAULT 'pending',
                created_by INTEGER NOT NULL,
                assigned_to INTEGER,
                department TEXT,
                branch_id INTEGER,
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (created_by) REFERENCES users(id),
                FOREIGN KEY (branch_id) REFERENCES branches(id)
            )
        """)

        # جدول الأدوار
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_roles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                role_name TEXT UNIQUE NOT NULL,
                role_name_ar TEXT NOT NULL,
                description TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # جدول الأقسام
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_departments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dept_name TEXT UNIQUE NOT NULL,
                dept_name_ar TEXT NOT NULL,
                description TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # جدول الحالات
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS system_statuses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                status_name TEXT UNIQUE NOT NULL,
                status_name_ar TEXT NOT NULL,
                status_color TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ✅ جدول أنواع الطلبات
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS request_types (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type_name TEXT UNIQUE NOT NULL,
                type_name_ar TEXT NOT NULL,
                description TEXT,
                is_active INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # جدول الصلاحيات
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_permissions (
                user_id INTEGER PRIMARY KEY,
                can_manage_users INTEGER DEFAULT 0,
                can_manage_branches INTEGER DEFAULT 0,
                can_manage_system_vars INTEGER DEFAULT 0,
                can_view_reports INTEGER DEFAULT 0,
                can_view_requests INTEGER DEFAULT 1,
                can_backup INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)

        # جدول الإشعارات
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                request_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                is_read INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(id),
                FOREIGN KEY (request_id) REFERENCES service_requests(id)
            )
        """)

        conn.commit()

        self._create_default_branches(conn)
        self._create_default_users(conn)
        self._create_default_roles(conn)
        self._create_default_departments(conn)
        self._create_default_statuses(conn)
        self._create_default_request_types(conn)

        conn.close()

    def _migrate_database(self):
        """إضافة أعمدة/جداول جديدة إذا ما موجودة"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Migration لـ can_backup
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_permissions'")
            if cursor.fetchone():
                cursor.execute("PRAGMA table_info(user_permissions)")
                columns = [column[1] for column in cursor.fetchall()]
                if 'can_backup' not in columns:
                    print("❌ can_backup column NOT found! Recreating table...")
                    cursor.execute("SELECT * FROM user_permissions")
                    old_data = cursor.fetchall()
                    cursor.execute("DROP TABLE user_permissions")
                    cursor.execute("""
                        CREATE TABLE user_permissions (
                            user_id INTEGER PRIMARY KEY,
                            can_manage_users INTEGER DEFAULT 0,
                            can_manage_branches INTEGER DEFAULT 0,
                            can_manage_system_vars INTEGER DEFAULT 0,
                            can_view_reports INTEGER DEFAULT 0,
                            can_view_requests INTEGER DEFAULT 1,
                            can_backup INTEGER DEFAULT 0,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (user_id) REFERENCES users(id)
                        )
                    """)
                    for row in old_data:
                        cursor.execute("""
                            INSERT INTO user_permissions 
                            (user_id, can_manage_users, can_manage_branches, can_manage_system_vars, 
                             can_view_reports, can_view_requests, can_backup, updated_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (row[0], row[1] if len(row)>1 else 0, row[2] if len(row)>2 else 0,
                              row[3] if len(row)>3 else 0, row[4] if len(row)>4 else 0,
                              row[5] if len(row)>5 else 1, 0,
                              row[6] if len(row)>6 else datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    conn.commit()
                    print("✅ can_backup migration done!")

            # ✅ Migration لـ request_types - يضيف الجدول إذا ناقص
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='request_types'")
            if not cursor.fetchone():
                print("❌ request_types table NOT found! Creating...")
                cursor.execute("""
                    CREATE TABLE request_types (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        type_name TEXT UNIQUE NOT NULL,
                        type_name_ar TEXT NOT NULL,
                        description TEXT,
                        is_active INTEGER DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                conn.commit()
                request_types = [
                    ('technical_support', 'دعم فني', 'طلب دعم فني'),
                    ('maintenance', 'صيانة', 'طلب صيانة'),
                    ('service', 'خدمة', 'طلب خدمة'),
                    ('complaint', 'شكوى', 'تقديم شكوى'),
                    ('other', 'أخرى', 'أي طلب آخر'),
                ]
                for rt in request_types:
                    try:
                        cursor.execute("INSERT INTO request_types (type_name, type_name_ar, description) VALUES (?, ?, ?)", rt)
                    except sqlite3.IntegrityError:
                        pass
                conn.commit()
                print("✅ request_types table created with defaults!")
            else:
                print("✅ request_types table already exists")

            conn.close()
        except Exception as e:
            print(f"Migration error: {e}")
            import traceback
            print(traceback.format_exc())

    def _create_default_branches(self, conn):
        cursor = conn.cursor()

        branches = [
            ('HQ - المقر الرئيسي', 'HQ-001', 'بغداد - المنصور', 'إدارة المقر', '07700000001'),
            ('فرع السعودن', 'SVD-001', 'بغداد - السعودن', 'مدير السعودن', '07700000002'),
            ('فرع الحارثية', 'HAR-001', 'بغداد - الحارثية', 'مدير الحارثية', '07700000003'),
            ('فرع السيدية', 'SYD-001', 'بغداد - السيدية', 'مدير السيدية', '07700000004'),
            ('فرع زيونة', 'ZYN-001', 'بغداد - زيونة', 'مدير زيونة', '07700000005'),
            ('فرع الدورة', 'DRA-001', 'بغداد - الدورة', 'مدير الدورة', '07700000006'),
            ('فرع كربلاء', 'KRB-001', 'كربلاء المقدسة', 'مدير كربلاء', '07700000007'),
            ('فرع بابل', 'BBL-001', 'بابل - الحلة', 'مدير بابل', '07700000008'),
            ('فرع البصرة', 'BSR-001', 'البصرة - العشار', 'مدير البصرة', '07700000009'),
            ('فرع كركوك', 'KRK-001', 'كركوك', 'مدير كركوك', '07700000010'),
            ('فرع أربيل', 'ERB-001', 'أربيل', 'مدير أربيل', '07700000011'),
            ('فرع تكريت', 'TKR-001', 'صلاح الدين - تكريت', 'مدير تكريت', '07700000012'),
            ('فرع المنصور', 'MNS-001', 'بغداد - المنصور', 'مدير المنصور', '07700000013'),
            ('فرع الموصل', 'MSL-001', 'نينوى - الموصل', 'مدير الموصل', '07700000014'),
            ('فرع ميسان', 'MYS-001', 'ميسان - العمارة', 'مدير ميسان', '07700000015'),
            ('فرع السليمانية', 'SLM-001', 'السليمانية', 'مدير السليمانية', '07700000016'),
            ('فرع النجف', 'NJF-001', 'النجف الأشرف', 'مدير النجف', '07700000017'),
            ('فرع الفلوجة', 'FLJ-001', 'الأنبار - الفلوجة', 'مدير الفلوجة', '07700000018'),
        ]

        for branch in branches:
            try:
                cursor.execute("""
                    INSERT INTO branches (name, code, location, manager_name, contact_phone)
                    VALUES (?, ?, ?, ?, ?)
                """, branch)
            except sqlite3.IntegrityError:
                pass

        conn.commit()

    def _create_default_users(self, conn):
        cursor = conn.cursor()

        default_users = [
            ('compliance', self.hash_password('compliance123'), 'مسؤول الامتثال', 'compliance@company.iq', 'compliance_officer', 'Compliance', 1),
            ('gm', self.hash_password('gm123'), 'المدير العام', 'gm@company.iq', 'general_manager', 'Management', 1),
            ('it_manager', self.hash_password('it123'), 'مدير IT', 'it@company.iq', 'department_head', 'IT', 1),
        ]

        for user in default_users:
            try:
                cursor.execute("""
                    INSERT INTO users (username, password, full_name, email, role, department, branch_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, user)
            except sqlite3.IntegrityError:
                pass

        conn.commit()

    def _create_default_roles(self, conn):
        cursor = conn.cursor()

        roles = [
            ('compliance_officer', 'مسؤول الامتثال', 'صلاحيات كاملة'),
            ('general_manager', 'مدير عام', 'إدارة الفروع والتقارير'),
            ('department_head', 'رئيس قسم', 'إدارة القسم'),
            ('employee', 'موظف', 'طلبات فقط'),
        ]

        for role in roles:
            try:
                cursor.execute("""
                    INSERT INTO system_roles (role_name, role_name_ar, description)
                    VALUES (?, ?, ?)
                """, role)
            except sqlite3.IntegrityError:
                pass

        conn.commit()

    def _create_default_departments(self, conn):
        cursor = conn.cursor()

        departments = [
            ('IT', 'تقنية المعلومات', 'قسم IT'),
            ('HR', 'الموارد البشرية', 'قسم HR'),
            ('Finance', 'المالية', 'قسم المالية'),
            ('Operations', 'العمليات', 'قسم العمليات'),
            ('Sales', 'المبيعات', 'قسم المبيعات'),
            ('Compliance', 'الامتثال', 'قسم الامتثال'),
            ('Management', 'الإدارة', 'الإدارة العليا'),
        ]

        for dept in departments:
            try:
                cursor.execute("""
                    INSERT INTO system_departments (dept_name, dept_name_ar, description)
                    VALUES (?, ?, ?)
                """, dept)
            except sqlite3.IntegrityError:
                pass

        conn.commit()

    def _create_default_statuses(self, conn):
        cursor = conn.cursor()

        statuses = [
            ('pending', 'قيد المعالجة', '#ff9800'),
            ('in_progress', 'قيد الإنجاز', '#ffc107'),
            ('completed', 'منجز', '#4caf50'),
            ('approved', 'موافق عليه', '#2196f3'),
            ('rejected', 'مرفوض', '#f44336'),
        ]

        for status in statuses:
            try:
                cursor.execute("""
                    INSERT INTO system_statuses (status_name, status_name_ar, status_color)
                    VALUES (?, ?, ?)
                """, status)
            except sqlite3.IntegrityError:
                pass

        conn.commit()

    def _create_default_request_types(self, conn):
        cursor = conn.cursor()

        request_types = [
            ('technical_support', 'دعم فني', 'طلب دعم فني'),
            ('maintenance', 'صيانة', 'طلب صيانة'),
            ('service', 'خدمة', 'طلب خدمة'),
            ('complaint', 'شكوى', 'تقديم شكوى'),
            ('other', 'أخرى', 'أي طلب آخر'),
        ]

        for rt in request_types:
            try:
                cursor.execute("""
                    INSERT INTO request_types (type_name, type_name_ar, description)
                    VALUES (?, ?, ?)
                """, rt)
            except sqlite3.IntegrityError:
                pass

        conn.commit()

    @staticmethod
    def hash_password(password: str) -> str:
        return hashlib.sha256(password.encode()).hexdigest()

    def verify_login(self, username: str, password: str) -> Optional[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        hashed_password = self.hash_password(password)

        cursor.execute("""
            SELECT * FROM users 
            WHERE username = ? AND password = ? AND is_active = 1
        """, (username, hashed_password))

        user = cursor.fetchone()
        conn.close()

        if user:
            return dict(user)
        return None

    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        user = cursor.fetchone()
        conn.close()

        if user:
            return dict(user)
        return None

    def get_user_permissions(self, user_id: int) -> Dict:
        """الحصول على صلاحيات المستخدم"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM user_permissions WHERE user_id = ?", (user_id,))
        perms = cursor.fetchone()
        conn.close()

        if perms:
            perms_dict = dict(perms)
            result = {}
            for key, value in perms_dict.items():
                if key == 'user_id':
                    result[key] = value
                elif key == 'updated_at':
                    result[key] = value
                else:
                    try:
                        result[key] = int(value) if value is not None else 0
                    except (ValueError, TypeError):
                        result[key] = 0
            return result
        else:
            return {
                'user_id': user_id,
                'can_manage_users': 0,
                'can_manage_branches': 0,
                'can_manage_system_vars': 0,
                'can_view_reports': 0,
                'can_view_requests': 1,
                'can_backup': 0
            }

    def set_user_permissions(self, user_id: int, permissions: Dict) -> bool:
        """تعيين صلاحيات المستخدم"""
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(user_permissions)")
        columns = [column[1] for column in cursor.fetchall()]
        print(f"DEBUG set_user_permissions - columns: {columns}")

        if 'can_backup' not in columns:
            print("ERROR: can_backup column missing! Running migration...")
            conn.close()
            self._migrate_database()
            conn = self.get_connection()
            cursor = conn.cursor()

        can_manage_users = int(permissions.get('can_manage_users', 0))
        can_manage_branches = int(permissions.get('can_manage_branches', 0))
        can_manage_system_vars = int(permissions.get('can_manage_system_vars', 0))
        can_view_reports = int(permissions.get('can_view_reports', 0))
        can_view_requests = int(permissions.get('can_view_requests', 1))
        can_backup = int(permissions.get('can_backup', 0))

        cursor.execute("""
            INSERT OR REPLACE INTO user_permissions 
            (user_id, can_manage_users, can_manage_branches, can_manage_system_vars, 
             can_view_reports, can_view_requests, can_backup, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            user_id,
            can_manage_users,
            can_manage_branches,
            can_manage_system_vars,
            can_view_reports,
            can_view_requests,
            can_backup,
        ))

        conn.commit()
        conn.close()

        return True

    def create_notification(self, user_id: int, request_id: int, message: str) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO notifications (user_id, request_id, message)
            VALUES (?, ?, ?)
        """, (user_id, request_id, message))

        notif_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return notif_id

    def get_unread_notifications(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT n.*, sr.title as request_title
            FROM notifications n
            JOIN service_requests sr ON n.request_id = sr.id
            WHERE n.user_id = ? AND n.is_read = 0
            ORDER BY n.created_at DESC
            LIMIT 10
        """, (user_id,))

        notifs = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return notifs

    def mark_notification_read(self, notif_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("UPDATE notifications SET is_read = 1 WHERE id = ?", (notif_id,))

        conn.commit()
        conn.close()

        return True

    def get_unread_count(self, user_id: int) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM notifications WHERE user_id = ? AND is_read = 0", (user_id,))
        count = cursor.fetchone()[0]
        conn.close()

        return count

    def get_all_branches(self, include_inactive=False) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        if include_inactive:
            cursor.execute("SELECT * FROM branches ORDER BY name")
        else:
            cursor.execute("SELECT * FROM branches WHERE is_active = 1 ORDER BY name")

        branches = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return branches

    def create_branch(self, data: Dict) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO branches (name, code, location, manager_name, contact_phone, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            data['name'],
            data['code'],
            data.get('location', ''),
            data.get('manager_name', ''),
            data.get('contact_phone', ''),
            data.get('is_active', 1)
        ))

        branch_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return branch_id

    def update_branch(self, branch_id: int, data: Dict) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE branches 
            SET name = ?, code = ?, location = ?, manager_name = ?, contact_phone = ?, is_active = ?
            WHERE id = ?
        """, (
            data['name'],
            data['code'],
            data.get('location', ''),
            data.get('manager_name', ''),
            data.get('contact_phone', ''),
            data.get('is_active', 1),
            branch_id
        ))

        conn.commit()
        conn.close()

        return True

    def toggle_branch_status(self, branch_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("UPDATE branches SET is_active = 1 - is_active WHERE id = ?", (branch_id,))

        conn.commit()
        conn.close()

        return True

    def delete_branch(self, branch_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM users WHERE branch_id = ?", (branch_id,))
        user_count = cursor.fetchone()[0]

        if user_count > 0:
            raise Exception(f"لا يمكن حذف الفرع لأنه مرتبط بـ {user_count} مستخدم")

        cursor.execute("DELETE FROM branches WHERE id = ?", (branch_id,))

        conn.commit()
        conn.close()

        return True

    def get_all_users(self, include_inactive=False) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        if include_inactive:
            cursor.execute("""
                SELECT u.*, b.name as branch_name 
                FROM users u
                LEFT JOIN branches b ON u.branch_id = b.id
                ORDER BY u.full_name
            """)
        else:
            cursor.execute("""
                SELECT u.*, b.name as branch_name 
                FROM users u
                LEFT JOIN branches b ON u.branch_id = b.id
                WHERE u.is_active = 1
                ORDER BY u.full_name
            """)

        users = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return users

    def create_user(self, data: Dict) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()

        hashed_password = self.hash_password(data['password'])

        cursor.execute("""
            INSERT INTO users (username, password, full_name, email, role, department, branch_id, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data['username'],
            hashed_password,
            data['full_name'],
            data.get('email', ''),
            data['role'],
            data['department'],
            data.get('branch_id'),
            data.get('is_active', 1)
        ))

        user_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return user_id

    def update_user(self, user_id: int, data: Dict) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        if 'password' in data and data['password']:
            hashed_password = self.hash_password(data['password'])
            cursor.execute("""
                UPDATE users 
                SET full_name = ?, email = ?, role = ?, department = ?, branch_id = ?, is_active = ?, password = ?
                WHERE id = ?
            """, (
                data['full_name'],
                data.get('email', ''),
                data['role'],
                data['department'],
                data.get('branch_id'),
                data.get('is_active', 1),
                hashed_password,
                user_id
            ))
        else:
            cursor.execute("""
                UPDATE users 
                SET full_name = ?, email = ?, role = ?, department = ?, branch_id = ?, is_active = ?
                WHERE id = ?
            """, (
                data['full_name'],
                data.get('email', ''),
                data['role'],
                data['department'],
                data.get('branch_id'),
                data.get('is_active', 1),
                user_id
            ))

        conn.commit()
        conn.close()

        return True

    def toggle_user_status(self, user_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("UPDATE users SET is_active = 1 - is_active WHERE id = ?", (user_id,))

        conn.commit()
        conn.close()

        return True

    def delete_user(self, user_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM user_permissions WHERE user_id = ?", (user_id,))
        cursor.execute("DELETE FROM notifications WHERE user_id = ?", (user_id,))
        cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))

        conn.commit()
        conn.close()

        return True

    def get_requests_by_user(self, user_id: int) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT sr.*, u.full_name as creator_name, b.name as branch_name
            FROM service_requests sr
            JOIN users u ON sr.created_by = u.id
            LEFT JOIN branches b ON sr.branch_id = b.id
            WHERE sr.created_by = ?
            ORDER BY sr.created_at DESC
        """, (user_id,))

        requests = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return requests

    def get_all_requests(self) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT sr.*, u.full_name as creator_name, b.name as branch_name
            FROM service_requests sr
            JOIN users u ON sr.created_by = u.id
            LEFT JOIN branches b ON sr.branch_id = b.id
            ORDER BY sr.created_at DESC
        """)

        requests = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return requests

    def get_request_by_id(self, request_id: int) -> Optional[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT sr.*, u.full_name as creator_name, b.name as branch_name
            FROM service_requests sr
            JOIN users u ON sr.created_by = u.id
            LEFT JOIN branches b ON sr.branch_id = b.id
            WHERE sr.id = ?
        """, (request_id,))

        request = cursor.fetchone()
        conn.close()

        if request:
            return dict(request)
        return None

    def create_request(self, data: Dict) -> int:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO service_requests 
            (request_type, title, description, priority, created_by, department, branch_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            data['request_type'],
            data['title'],
            data.get('description', ''),
            data.get('priority', 'medium'),
            data['created_by'],
            data.get('department', ''),
            data.get('branch_id')
        ))

        request_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return request_id

    def update_request_status(self, request_id: int, status: str, notes: str = '') -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE service_requests 
            SET status = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (status, notes, request_id))

        conn.commit()
        conn.close()

        return True

    def delete_request(self, request_id: int) -> bool:
        """حذف طلب من قاعدة البيانات"""
        conn = self.get_connection()
        cursor = conn.cursor()

        # حذف الإشعارات المرتبطة بالطلب أولاً
        cursor.execute("DELETE FROM notifications WHERE request_id = ?", (request_id,))
        # حذف الطلب
        cursor.execute("DELETE FROM service_requests WHERE id = ?", (request_id,))

        conn.commit()
        conn.close()

        return True

    def get_all_roles(self) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM system_roles WHERE is_active = 1 ORDER BY role_name_ar")
        roles = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return roles

    def add_role(self, role_name: str, role_name_ar: str, description: str = '') -> int:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO system_roles (role_name, role_name_ar, description)
            VALUES (?, ?, ?)
        """, (role_name, role_name_ar, description))

        role_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return role_id

    def update_role(self, role_id: int, role_name: str, role_name_ar: str, description: str = '') -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE system_roles SET role_name = ?, role_name_ar = ?, description = ? WHERE id = ?
        """, (role_name, role_name_ar, description, role_id))

        conn.commit()
        conn.close()
        return True

    def delete_role(self, role_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM system_roles WHERE id = ?", (role_id,))

        conn.commit()
        conn.close()
        return True

    def get_all_departments(self) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM system_departments WHERE is_active = 1 ORDER BY dept_name_ar")
        departments = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return departments

    def add_department(self, dept_name: str, dept_name_ar: str, description: str = '') -> int:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO system_departments (dept_name, dept_name_ar, description)
            VALUES (?, ?, ?)
        """, (dept_name, dept_name_ar, description))

        dept_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return dept_id

    def update_department(self, dept_id: int, dept_name: str, dept_name_ar: str, description: str = '') -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE system_departments SET dept_name = ?, dept_name_ar = ?, description = ? WHERE id = ?
        """, (dept_name, dept_name_ar, description, dept_id))

        conn.commit()
        conn.close()
        return True

    def delete_department(self, dept_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM system_departments WHERE id = ?", (dept_id,))

        conn.commit()
        conn.close()
        return True

    def get_all_statuses(self) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM system_statuses WHERE is_active = 1 ORDER BY id")
        statuses = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return statuses

    def add_status(self, status_name: str, status_name_ar: str, status_color: str = '#000000') -> int:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO system_statuses (status_name, status_name_ar, status_color)
            VALUES (?, ?, ?)
        """, (status_name, status_name_ar, status_color))

        status_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return status_id

    def update_status(self, status_id: int, status_name: str, status_name_ar: str, status_color: str = '#000000') -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE system_statuses SET status_name = ?, status_name_ar = ?, status_color = ? WHERE id = ?
        """, (status_name, status_name_ar, status_color, status_id))

        conn.commit()
        conn.close()
        return True

    def delete_status(self, status_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM system_statuses WHERE id = ?", (status_id,))

        conn.commit()
        conn.close()
        return True

    def get_all_request_types(self) -> List[Dict]:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM request_types WHERE is_active = 1 ORDER BY type_name_ar")
        types = [dict(row) for row in cursor.fetchall()]
        conn.close()

        return types

    def add_request_type(self, type_name: str, type_name_ar: str, description: str = '') -> int:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO request_types (type_name, type_name_ar, description)
            VALUES (?, ?, ?)
        """, (type_name, type_name_ar, description))

        type_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return type_id

    def update_request_type(self, type_id: int, type_name: str, type_name_ar: str, description: str = '') -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            UPDATE request_types SET type_name = ?, type_name_ar = ?, description = ? WHERE id = ?
        """, (type_name, type_name_ar, description, type_id))

        conn.commit()
        conn.close()
        return True

    def delete_request_type(self, type_id: int) -> bool:
        conn = self.get_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM request_types WHERE id = ?", (type_id,))

        conn.commit()
        conn.close()
        return True

    def get_dashboard_stats(self, user_id: int = None) -> Dict:
        conn = self.get_connection()
        cursor = conn.cursor()

        stats = {}

        if user_id:
            cursor.execute("SELECT COUNT(*) FROM service_requests WHERE created_by = ?", (user_id,))
            stats['total_requests'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM service_requests WHERE created_by = ? AND status = 'pending'", (user_id,))
            stats['pending_requests'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM service_requests WHERE created_by = ? AND status = 'in_progress'", (user_id,))
            stats['in_progress_requests'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM service_requests WHERE created_by = ? AND status = 'completed'", (user_id,))
            stats['completed_requests'] = cursor.fetchone()[0]
        else:
            cursor.execute("SELECT COUNT(*) FROM service_requests")
            stats['total_requests'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM service_requests WHERE status = 'pending'")
            stats['pending_requests'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM service_requests WHERE status = 'in_progress'")
            stats['in_progress_requests'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM service_requests WHERE status = 'completed'")
            stats['completed_requests'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM branches WHERE is_active = 1")
            stats['total_branches'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = 1")
            stats['total_users'] = cursor.fetchone()[0]

        conn.close()
        return stats

    def backup_database(self, backup_dir: str = None) -> str:
        if backup_dir is None:
            backup_dir = 'backups'

        os.makedirs(backup_dir, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(backup_dir, f"backup_{timestamp}.db")

        shutil.copy2(self.db_path, backup_path)
        return backup_path

    def restore_database(self, backup_path: str) -> bool:
        try:
            if not os.path.exists(backup_path):
                return False

            shutil.copy2(backup_path, self.db_path)
            
            # ✅ بعد الاسترجاع، تأكد إن الجداول الجديدة موجودة
            self._migrate_database()
            
            return True
        except Exception as e:
            print(f"Error restoring database: {e}")
            return False
