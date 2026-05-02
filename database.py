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
        
        # تأكد المجلد موجود
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        
        self.db_path = db_path
        self.init_database()
        self._migrate_database()  # ← تحقق من الأعمدة الجديدة
    
    def get_connection(self):
        """إنشاء اتصال بقاعدة البيانات"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
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
        
        # جدول الصلاحيات - معدل! أضفنا can_backup
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
        
        # إضافة بيانات افتراضية
        self._create_default_branches(conn)
        self._create_default_users(conn)
        self._create_default_roles(conn)
        self._create_default_departments(conn)
        self._create_default_statuses(conn)
        
        conn.close()
    
    def _migrate_database(self):
        """إضافة أعمدة جديدة إذا ما موجودة"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # التحقق من وجود عمود can_backup
            cursor.execute("PRAGMA table_info(user_permissions)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'can_backup' not in columns:
                cursor.execute("ALTER TABLE user_permissions ADD COLUMN can_backup INTEGER DEFAULT 0")
                conn.commit()
                print("✅ Added can_backup column")
            
            conn.close()
        except Exception as e:
            print(f"Migration error: {e}")
    
    def _create_default_branches(self, conn):
        """إنشاء الفروع الـ 18"""
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
        """إنشاء مستخدمين افتراضيين"""
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
        """إنشاء الأدوار الافتراضية"""
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
        """إنشاء الأقسام الافتراضية"""
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
        """إنشاء حالات الطلبات"""
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
    
    @staticmethod
    def hash_password(password: str) -> str:
        """تشفير كلمة المرور"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def verify_login(self, username: str, password: str) -> Optional[Dict]:
        """التحقق من تسجيل الدخول"""
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
        """الحصول على مستخدم بالمعرف"""
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
            return dict(perms)
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
        
        # التحقق من وجود can_backup
        cursor.execute("PRAGMA table_info(user_permissions)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'can_backup' in columns:
            cursor.execute("""
                INSERT OR REPLACE INTO user_permissions 
                (user_id, can_manage_users, can_manage_branches, can_manage_system_vars, can_view_reports, can_view_requests, can_backup, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                user_id,
                permissions.get('can_manage_users', 0),
                permissions.get('can_manage_branches', 0),
                permissions.get('can_manage_system_vars', 0),
                permissions.get('can_view_reports', 0),
                permissions.get('can_view_requests', 1),
                permissions.get('can_backup', 0)
            ))
        else:
            cursor.execute("""
                INSERT OR REPLACE INTO user_permissions 
                (user_id, can_manage_users, can_manage_branches, can_manage_system_vars, can_view_reports, can_view_requests, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                user_id,
                permissions.get('can_manage_users', 0),
                permissions.get('can_manage_branches', 0),
                permissions.get('can_manage_system_vars', 0),
                permissions.get('can_view_reports', 0),
                permissions.get('can_view_requests', 1)
            ))
        
        conn.commit()
        conn.close()
        
        return True
    
    def create_notification(self, user_id: int, request_id: int, message: str) -> int:
        """إنشاء إشعار جديد"""
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
        """الحصول على الإشعارات غير المقروءة"""
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
        """وضع علامة مقروء على الإشعار"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("UPDATE notifications SET is_read = 1 WHERE id = ?", (notif_id,))
        
        conn.commit()
        conn.close()
        
        return True
    
    def get_unread_count(self, user_id: int) -> int:
        """عدد الإشعارات غير المقروءة"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM notifications WHERE user_id = ? AND is_read = 0", (user_id,))
        count = cursor.fetchone()[0]
        conn.close()
        
        return count
    
    def get_all_branches(self, include_inactive=False) -> List[Dict]:
        """الحصول على جميع الفروع"""
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
        """إضافة فرع جديد"""
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
        """تحديث فرع"""
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
        """تبديل حالة الفرع"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("UPDATE branches SET is_active = 1 - is_active WHERE id = ?", (branch_id,))
        
        conn.commit()
        conn.close()
        
        return True
    
    def get_all_users(self, include_inactive=False) -> List[Dict]:
        """الحصول على جميع المستخدمين"""
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
        """إضافة مستخدم جديد"""
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
        """تحديث مستخدم"""
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
        """تبديل حالة المستخدم"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("UPDATE users SET is_active = 1 - is_active WHERE id = ?", (user_id,))
        
        conn.commit()
        conn.close()
        
        return True
    
    def delete_user(self, user_id: int) -> bool:
        """حذف مستخدم نهائياً"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM user_permissions WHERE user_id = ?", (user_id,))
        cursor.execute("DELETE FROM notifications WHERE user_id = ?", (user_id,))
        cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
        
        conn.commit()
        conn.close()
        
        return True
    
    def get_requests_by_user(self, user_id: int) -> List[Dict]:
        """الحصول على طلبات المستخدم"""
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
        """الحصول على جميع الطلبات"""
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
        """الحصول على تفاصيل طلب"""
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
        """إنشاء طلب جديد"""
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
        """تحديث حالة طلب"""
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
    
    def get_all_roles(self) -> List[Dict]:
        """الحصول على جميع الأدوار"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM system_roles WHERE is_active = 1 ORDER BY role_name_ar")
        roles = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return roles
    
    def get_all_departments(self) -> List[Dict]:
        """الحصول على جميع الأقسام"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM system_departments WHERE is_active = 1 ORDER BY dept_name_ar")
        departments = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return departments
    
    def get_all_statuses(self) -> List[Dict]:
        """الحصول على جميع الحالات"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM system_statuses WHERE is_active = 1 ORDER BY id")
        statuses = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return statuses
    
    def add_role(self, role_name: str, role_name_ar: str, description: str = '') -> int:
        """إضافة دور جديد"""
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
    
    def add_department(self, dept_name: str, dept_name_ar: str, description: str = '') -> int:
        """إضافة قسم جديد"""
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
    
    def get_dashboard_stats(self, user_id: int = None) -> Dict:
        """إحصائيات Dashboard"""
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
        """إنشاء نسخة احتياطية"""
        if backup_dir is None:
            backup_dir = 'backups'
        
        os.makedirs(backup_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(backup_dir, f"backup_{timestamp}.db")
        
        shutil.copy2(self.db_path, backup_path)
        return backup_path
    
    def restore_database(self, backup_path: str) -> bool:
        """استرجاع قاعدة البيانات من نسخة احتياطية"""
        try:
            if not os.path.exists(backup_path):
                return False
            
            shutil.copy2(backup_path, self.db_path)
            return True
        except Exception as e:
            print(f"Error restoring database: {e}")
            return False
