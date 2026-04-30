"""
Database Management Module - v4.0 with Permissions & Notifications - PostgreSQL Version
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import hashlib
from datetime import datetime
from typing import Dict, List, Optional

class Database:
    """إدارة قاعدة البيانات"""
    
    def __init__(self):
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            raise Exception("DATABASE_URL not found!")
        
        self.engine = create_engine(database_url, poolclass=NullPool)
        self.init_database()
    
    def get_connection(self):
        """إنشاء اتصال بقاعدة البيانات"""
        return self.engine.connect()
    
    def _row_to_dict(self, row):
        """تحويل Row إلى dict"""
        if row is None:
            return None
        return {key: value for key, value in row._mapping.items()}
    
    def init_database(self):
        """تهيئة قاعدة البيانات"""
        conn = self.get_connection()
        
        try:
            # جدول المستخدمين
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(100) UNIQUE NOT NULL,
                    password VARCHAR(255) NOT NULL,
                    full_name VARCHAR(200) NOT NULL,
                    email VARCHAR(200),
                    role VARCHAR(50) NOT NULL,
                    department VARCHAR(100) NOT NULL,
                    branch_id INTEGER,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # جدول الفروع
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS branches (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    code VARCHAR(20) UNIQUE NOT NULL,
                    location VARCHAR(300),
                    manager_name VARCHAR(200),
                    contact_phone VARCHAR(50),
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # جدول الطلبات
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS service_requests (
                    id SERIAL PRIMARY KEY,
                    request_type VARCHAR(100) NOT NULL,
                    title VARCHAR(300) NOT NULL,
                    description TEXT,
                    priority VARCHAR(50) DEFAULT 'medium',
                    status VARCHAR(50) DEFAULT 'pending',
                    created_by INTEGER NOT NULL,
                    assigned_to INTEGER,
                    department VARCHAR(100),
                    branch_id INTEGER,
                    notes TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # جدول الأدوار
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS system_roles (
                    id SERIAL PRIMARY KEY,
                    role_name VARCHAR(100) UNIQUE NOT NULL,
                    role_name_ar VARCHAR(200) NOT NULL,
                    description TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # جدول الأقسام
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS system_departments (
                    id SERIAL PRIMARY KEY,
                    dept_name VARCHAR(100) UNIQUE NOT NULL,
                    dept_name_ar VARCHAR(200) NOT NULL,
                    description TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # جدول الحالات
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS system_statuses (
                    id SERIAL PRIMARY KEY,
                    status_name VARCHAR(100) UNIQUE NOT NULL,
                    status_name_ar VARCHAR(200) NOT NULL,
                    status_color VARCHAR(20),
                    is_active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # جدول الصلاحيات
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_permissions (
                    user_id INTEGER PRIMARY KEY,
                    can_manage_users INTEGER DEFAULT 0,
                    can_manage_branches INTEGER DEFAULT 0,
                    can_manage_system_vars INTEGER DEFAULT 0,
                    can_view_reports INTEGER DEFAULT 0,
                    can_view_requests INTEGER DEFAULT 1,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # جدول الإشعارات
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    request_id INTEGER NOT NULL,
                    message TEXT NOT NULL,
                    is_read INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.commit()
            
            # إضافة بيانات افتراضية
            self._create_default_branches(conn)
            self._create_default_users(conn)
            self._create_default_roles(conn)
            self._create_default_departments(conn)
            self._create_default_statuses(conn)
            
        except Exception as e:
            conn.rollback()
            print(f"Database Error: {e}")
        finally:
            conn.close()
    
    def _create_default_branches(self, conn):
        """إنشاء الفروع الـ 18"""
        branches = [
            ('HQ - المقر الرئيسي', 'HQ-001', 'بغداد - المنصور', 'إدارة المقر', '07700000001'),
            ('فرع السعدون', 'SVD-001', 'بغداد - السعدون', 'مدير السعدون', '07700000002'),
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
        
        result = conn.execute(text("SELECT COUNT(*) as count FROM branches")).fetchone()
        
        if result[0] == 0:
            for branch in branches:
                conn.execute(text("""
                    INSERT INTO branches (name, code, location, manager_name, contact_phone)
                    VALUES (:name, :code, :location, :manager, :phone)
                """), {
                    'name': branch[0],
                    'code': branch[1],
                    'location': branch[2],
                    'manager': branch[3],
                    'phone': branch[4]
                })
            conn.commit()
    
    def _create_default_users(self, conn):
        """إنشاء مستخدمين افتراضيين"""
        default_users = [
            ('compliance', self.hash_password('compliance123'), 'مسؤول الامتثال', 'compliance@company.iq', 'compliance_officer', 'Compliance', 1),
            ('gm', self.hash_password('gm123'), 'المدير العام', 'gm@company.iq', 'general_manager', 'Management', 1),
            ('it_manager', self.hash_password('it123'), 'مدير IT', 'it@company.iq', 'department_head', 'IT', 1),
        ]
        
        result = conn.execute(text("SELECT COUNT(*) as count FROM users")).fetchone()
        
        if result[0] == 0:
            for user in default_users:
                conn.execute(text("""
                    INSERT INTO users (username, password, full_name, email, role, department, branch_id)
                    VALUES (:username, :password, :full_name, :email, :role, :department, :branch_id)
                """), {
                    'username': user[0],
                    'password': user[1],
                    'full_name': user[2],
                    'email': user[3],
                    'role': user[4],
                    'department': user[5],
                    'branch_id': user[6]
                })
            conn.commit()
    
    def _create_default_roles(self, conn):
        """إنشاء الأدوار الافتراضية"""
        roles = [
            ('compliance_officer', 'مسؤول الامتثال', 'صلاحيات كاملة'),
            ('general_manager', 'مدير عام', 'إدارة الفروع والتقارير'),
            ('department_head', 'رئيس قسم', 'إدارة القسم'),
            ('employee', 'موظف', 'طلبات فقط'),
        ]
        
        result = conn.execute(text("SELECT COUNT(*) as count FROM system_roles")).fetchone()
        
        if result[0] == 0:
            for role in roles:
                conn.execute(text("""
                    INSERT INTO system_roles (role_name, role_name_ar, description)
                    VALUES (:role_name, :role_name_ar, :description)
                """), {
                    'role_name': role[0],
                    'role_name_ar': role[1],
                    'description': role[2]
                })
            conn.commit()
    
    def _create_default_departments(self, conn):
        """إنشاء الأقسام الافتراضية"""
        departments = [
            ('IT', 'تقنية المعلومات', 'قسم IT'),
            ('HR', 'الموارد البشرية', 'قسم HR'),
            ('Finance', 'المالية', 'قسم المالية'),
            ('Operations', 'العمليات', 'قسم العمليات'),
            ('Sales', 'المبيعات', 'قسم المبيعات'),
            ('Compliance', 'الامتثال', 'قسم الامتثال'),
            ('Management', 'الإدارة', 'الإدارة العليا'),
        ]
        
        result = conn.execute(text("SELECT COUNT(*) as count FROM system_departments")).fetchone()
        
        if result[0] == 0:
            for dept in departments:
                conn.execute(text("""
                    INSERT INTO system_departments (dept_name, dept_name_ar, description)
                    VALUES (:dept_name, :dept_name_ar, :description)
                """), {
                    'dept_name': dept[0],
                    'dept_name_ar': dept[1],
                    'description': dept[2]
                })
            conn.commit()
    
    def _create_default_statuses(self, conn):
        """إنشاء حالات الطلبات"""
        statuses = [
            ('pending', 'قيد المعالجة', '#ff9800'),
            ('in_progress', 'قيد الإنجاز', '#ffc107'),
            ('completed', 'منجز', '#4caf50'),
            ('approved', 'موافق عليه', '#2196f3'),
            ('rejected', 'مرفوض', '#f44336'),
        ]
        
        result = conn.execute(text("SELECT COUNT(*) as count FROM system_statuses")).fetchone()
        
        if result[0] == 0:
            for status in statuses:
                conn.execute(text("""
                    INSERT INTO system_statuses (status_name, status_name_ar, status_color)
                    VALUES (:status_name, :status_name_ar, :status_color)
                """), {
                    'status_name': status[0],
                    'status_name_ar': status[1],
                    'status_color': status[2]
                })
            conn.commit()
    
    @staticmethod
    def hash_password(password: str) -> str:
        """تشفير كلمة المرور"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def verify_login(self, username: str, password: str) -> Optional[Dict]:
        """التحقق من تسجيل الدخول"""
        conn = self.get_connection()
        
        hashed_password = self.hash_password(password)
        
        result = conn.execute(text("""
            SELECT * FROM users 
            WHERE username = :username AND password = :password AND is_active = 1
        """), {'username': username, 'password': hashed_password}).fetchone()
        
        conn.close()
        
        if result:
            return self._row_to_dict(result)
        return None
    
    def get_user_by_id(self, user_id: int) -> Optional[Dict]:
        """الحصول على مستخدم بالمعرف"""
        conn = self.get_connection()
        
        result = conn.execute(text("SELECT * FROM users WHERE id = :id"), {'id': user_id}).fetchone()
        
        conn.close()
        
        if result:
            return self._row_to_dict(result)
        return None
    
    # ==================== الصلاحيات ====================
    
    def get_user_permissions(self, user_id: int) -> Dict:
        """الحصول على صلاحيات المستخدم"""
        conn = self.get_connection()
        
        result = conn.execute(text("SELECT * FROM user_permissions WHERE user_id = :id"), {'id': user_id}).fetchone()
        
        conn.close()
        
        if result:
            return self._row_to_dict(result)
        else:
            return {
                'user_id': user_id,
                'can_manage_users': 0,
                'can_manage_branches': 0,
                'can_manage_system_vars': 0,
                'can_view_reports': 0,
                'can_view_requests': 1
            }
    
    def set_user_permissions(self, user_id: int, permissions: Dict) -> bool:
        """تعيين صلاحيات المستخدم"""
        conn = self.get_connection()
        
        try:
            existing = conn.execute(text("SELECT user_id FROM user_permissions WHERE user_id = :id"), {'id': user_id}).fetchone()
            
            if existing:
                conn.execute(text("""
                    UPDATE user_permissions 
                    SET can_manage_users = :users, can_manage_branches = :branches, 
                        can_manage_system_vars = :vars, can_view_reports = :reports, 
                        can_view_requests = :requests, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = :id
                """), {
                    'users': permissions.get('can_manage_users', 0),
                    'branches': permissions.get('can_manage_branches', 0),
                    'vars': permissions.get('can_manage_system_vars', 0),
                    'reports': permissions.get('can_view_reports', 0),
                    'requests': permissions.get('can_view_requests', 1),
                    'id': user_id
                })
            else:
                conn.execute(text("""
                    INSERT INTO user_permissions 
                    (user_id, can_manage_users, can_manage_branches, can_manage_system_vars, can_view_reports, can_view_requests)
                    VALUES (:id, :users, :branches, :vars, :reports, :requests)
                """), {
                    'id': user_id,
                    'users': permissions.get('can_manage_users', 0),
                    'branches': permissions.get('can_manage_branches', 0),
                    'vars': permissions.get('can_manage_system_vars', 0),
                    'reports': permissions.get('can_view_reports', 0),
                    'requests': permissions.get('can_view_requests', 1)
                })
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    # ==================== الإشعارات ====================
    
    def create_notification(self, user_id: int, request_id: int, message: str) -> int:
        """إنشاء إشعار جديد"""
        conn = self.get_connection()
        
        try:
            result = conn.execute(text("""
                INSERT INTO notifications (user_id, request_id, message)
                VALUES (:user_id, :request_id, :message)
                RETURNING id
            """), {'user_id': user_id, 'request_id': request_id, 'message': message})
            
            notif_id = result.fetchone()[0]
            conn.commit()
            return notif_id
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_unread_notifications(self, user_id: int) -> List[Dict]:
        """الحصول على الإشعارات غير المقروءة"""
        conn = self.get_connection()
        
        result = conn.execute(text("""
            SELECT n.*, sr.title as request_title
            FROM notifications n
            JOIN service_requests sr ON n.request_id = sr.id
            WHERE n.user_id = :id AND n.is_read = 0
            ORDER BY n.created_at DESC
            LIMIT 10
        """), {'id': user_id}).fetchall()
        
        conn.close()
        
        return [self._row_to_dict(row) for row in result]
    
    def mark_notification_read(self, notif_id: int) -> bool:
        """وضع علامة مقروء على الإشعار"""
        conn = self.get_connection()
        
        try:
            conn.execute(text("UPDATE notifications SET is_read = 1 WHERE id = :id"), {'id': notif_id})
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_unread_count(self, user_id: int) -> int:
        """عدد الإشعارات غير المقروءة"""
        conn = self.get_connection()
        
        result = conn.execute(text("SELECT COUNT(*) FROM notifications WHERE user_id = :id AND is_read = 0"), {'id': user_id}).fetchone()
        
        conn.close()
        
        return result[0] if result else 0
    
    # ==================== باقي الوظائف ====================
    
    def get_all_branches(self, include_inactive=False) -> List[Dict]:
        """الحصول على جميع الفروع"""
        conn = self.get_connection()
        
        if include_inactive:
            result = conn.execute(text("SELECT * FROM branches ORDER BY name")).fetchall()
        else:
            result = conn.execute(text("SELECT * FROM branches WHERE is_active = 1 ORDER BY name")).fetchall()
        
        conn.close()
        
        return [self._row_to_dict(row) for row in result]
    
    def create_branch(self, data: Dict) -> int:
        """إضافة فرع جديد"""
        conn = self.get_connection()
        
        try:
            result = conn.execute(text("""
                INSERT INTO branches (name, code, location, manager_name, contact_phone, is_active)
                VALUES (:name, :code, :location, :manager, :phone, :active)
                RETURNING id
            """), {
                'name': data['name'],
                'code': data['code'],
                'location': data.get('location', ''),
                'manager': data.get('manager_name', ''),
                'phone': data.get('contact_phone', ''),
                'active': data.get('is_active', 1)
            })
            
            branch_id = result.fetchone()[0]
            conn.commit()
            return branch_id
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def update_branch(self, branch_id: int, data: Dict) -> bool:
        """تحديث فرع"""
        conn = self.get_connection()
        
        try:
            conn.execute(text("""
                UPDATE branches 
                SET name = :name, code = :code, location = :location, 
                    manager_name = :manager, contact_phone = :phone, is_active = :active
                WHERE id = :id
            """), {
                'name': data['name'],
                'code': data['code'],
                'location': data.get('location', ''),
                'manager': data.get('manager_name', ''),
                'phone': data.get('contact_phone', ''),
                'active': data.get('is_active', 1),
                'id': branch_id
            })
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def toggle_branch_status(self, branch_id: int) -> bool:
        """تبديل حالة الفرع"""
        conn = self.get_connection()
        
        try:
            conn.execute(text("UPDATE branches SET is_active = 1 - is_active WHERE id = :id"), {'id': branch_id})
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_all_users(self, include_inactive=False) -> List[Dict]:
        """الحصول على جميع المستخدمين"""
        conn = self.get_connection()
        
        if include_inactive:
            result = conn.execute(text("""
                SELECT u.*, b.name as branch_name 
                FROM users u
                LEFT JOIN branches b ON u.branch_id = b.id
                ORDER BY u.full_name
            """)).fetchall()
        else:
            result = conn.execute(text("""
                SELECT u.*, b.name as branch_name 
                FROM users u
                LEFT JOIN branches b ON u.branch_id = b.id
                WHERE u.is_active = 1
                ORDER BY u.full_name
            """)).fetchall()
        
        conn.close()
        
        return [self._row_to_dict(row) for row in result]
    
    def create_user(self, data: Dict) -> int:
        """إضافة مستخدم جديد"""
        conn = self.get_connection()
        
        try:
            hashed_password = self.hash_password(data['password'])
            
            result = conn.execute(text("""
                INSERT INTO users (username, password, full_name, email, role, department, branch_id, is_active)
                VALUES (:username, :password, :full_name, :email, :role, :department, :branch_id, :active)
                RETURNING id
            """), {
                'username': data['username'],
                'password': hashed_password,
                'full_name': data['full_name'],
                'email': data.get('email', ''),
                'role': data['role'],
                'department': data['department'],
                'branch_id': data.get('branch_id'),
                'active': data.get('is_active', 1)
            })
            
            user_id = result.fetchone()[0]
            conn.commit()
            return user_id
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def update_user(self, user_id: int, data: Dict) -> bool:
        """تحديث مستخدم"""
        conn = self.get_connection()
        
        try:
            if 'password' in data and data['password']:
                hashed_password = self.hash_password(data['password'])
                conn.execute(text("""
                    UPDATE users 
                    SET full_name = :full_name, email = :email, role = :role, 
                        department = :department, branch_id = :branch_id, is_active = :active, password = :password
                    WHERE id = :id
                """), {
                    'full_name': data['full_name'],
                    'email': data.get('email', ''),
                    'role': data['role'],
                    'department': data['department'],
                    'branch_id': data.get('branch_id'),
                    'active': data.get('is_active', 1),
                    'password': hashed_password,
                    'id': user_id
                })
            else:
                conn.execute(text("""
                    UPDATE users 
                    SET full_name = :full_name, email = :email, role = :role, 
                        department = :department, branch_id = :branch_id, is_active = :active
                    WHERE id = :id
                """), {
                    'full_name': data['full_name'],
                    'email': data.get('email', ''),
                    'role': data['role'],
                    'department': data['department'],
                    'branch_id': data.get('branch_id'),
                    'active': data.get('is_active', 1),
                    'id': user_id
                })
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def toggle_user_status(self, user_id: int) -> bool:
        """تبديل حالة المستخدم"""
        conn = self.get_connection()
        
        try:
            conn.execute(text("UPDATE users SET is_active = 1 - is_active WHERE id = :id"), {'id': user_id})
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def delete_user(self, user_id: int) -> bool:
        """حذف مستخدم نهائياً"""
        conn = self.get_connection()
        
        try:
            conn.execute(text("DELETE FROM user_permissions WHERE user_id = :id"), {'id': user_id})
            conn.execute(text("DELETE FROM notifications WHERE user_id = :id"), {'id': user_id})
            conn.execute(text("DELETE FROM users WHERE id = :id"), {'id': user_id})
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_requests_by_user(self, user_id: int) -> List[Dict]:
        """الحصول على طلبات المستخدم"""
        conn = self.get_connection()
        
        result = conn.execute(text("""
            SELECT sr.*, u.full_name as creator_name, b.name as branch_name
            FROM service_requests sr
            JOIN users u ON sr.created_by = u.id
            LEFT JOIN branches b ON sr.branch_id = b.id
            WHERE sr.created_by = :id
            ORDER BY sr.created_at DESC
        """), {'id': user_id}).fetchall()
        
        conn.close()
        
        return [self._row_to_dict(row) for row in result]
    
    def get_all_requests(self) -> List[Dict]:
        """الحصول على جميع الطلبات"""
        conn = self.get_connection()
        
        result = conn.execute(text("""
            SELECT sr.*, u.full_name as creator_name, b.name as branch_name
            FROM service_requests sr
            JOIN users u ON sr.created_by = u.id
            LEFT JOIN branches b ON sr.branch_id = b.id
            ORDER BY sr.created_at DESC
        """)).fetchall()
        
        conn.close()
        
        return [self._row_to_dict(row) for row in result]
    
    def get_request_by_id(self, request_id: int) -> Optional[Dict]:
        """الحصول على تفاصيل طلب"""
        conn = self.get_connection()
        
        result = conn.execute(text("""
            SELECT sr.*, u.full_name as creator_name, b.name as branch_name
            FROM service_requests sr
            JOIN users u ON sr.created_by = u.id
            LEFT JOIN branches b ON sr.branch_id = b.id
            WHERE sr.id = :id
        """), {'id': request_id}).fetchone()
        
        conn.close()
        
        if result:
            return self._row_to_dict(result)
        return None
    
    def create_request(self, data: Dict) -> int:
        """إنشاء طلب جديد"""
        conn = self.get_connection()
        
        try:
            result = conn.execute(text("""
                INSERT INTO service_requests 
                (request_type, title, description, priority, created_by, department, branch_id)
                VALUES (:type, :title, :desc, :priority, :created_by, :dept, :branch_id)
                RETURNING id
            """), {
                'type': data['request_type'],
                'title': data['title'],
                'desc': data.get('description', ''),
                'priority': data.get('priority', 'medium'),
                'created_by': data['created_by'],
                'dept': data.get('department', ''),
                'branch_id': data.get('branch_id')
            })
            
            request_id = result.fetchone()[0]
            conn.commit()
            return request_id
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def update_request_status(self, request_id: int, status: str, notes: str = '') -> bool:
        """تحديث حالة طلب"""
        conn = self.get_connection()
        
        try:
            conn.execute(text("""
                UPDATE service_requests 
                SET status = :status, notes = :notes, updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """), {'status': status, 'notes': notes, 'id': request_id})
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_all_roles(self) -> List[Dict]:
        """الحصول على جميع الأدوار"""
        conn = self.get_connection()
        
        result = conn.execute(text("SELECT * FROM system_roles WHERE is_active = 1 ORDER BY role_name_ar")).fetchall()
        
        conn.close()
        
        return [self._row_to_dict(row) for row in result]
    
    def get_all_departments(self) -> List[Dict]:
        """الحصول على جميع الأقسام"""
        conn = self.get_connection()
        
        result = conn.execute(text("SELECT * FROM system_departments WHERE is_active = 1 ORDER BY dept_name_ar")).fetchall()
        
        conn.close()
        
        return [self._row_to_dict(row) for row in result]
    
    def get_all_statuses(self) -> List[Dict]:
        """الحصول على جميع الحالات"""
        conn = self.get_connection()
        
        result = conn.execute(text("SELECT * FROM system_statuses WHERE is_active = 1 ORDER BY id")).fetchall()
        
        conn.close()
        
        return [self._row_to_dict(row) for row in result]
    
    def add_role(self, role_name: str, role_name_ar: str, description: str = '') -> int:
        """إضافة دور جديد"""
        conn = self.get_connection()
        
        try:
            result = conn.execute(text("""
                INSERT INTO system_roles (role_name, role_name_ar, description)
                VALUES (:role_name, :role_name_ar, :description)
                RETURNING id
            """), {'role_name': role_name, 'role_name_ar': role_name_ar, 'description': description})
            
            role_id = result.fetchone()[0]
            conn.commit()
            return role_id
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def add_department(self, dept_name: str, dept_name_ar: str, description: str = '') -> int:
        """إضافة قسم جديد"""
        conn = self.get_connection()
        
        try:
            result = conn.execute(text("""
                INSERT INTO system_departments (dept_name, dept_name_ar, description)
                VALUES (:dept_name, :dept_name_ar, :description)
                RETURNING id
            """), {'dept_name': dept_name, 'dept_name_ar': dept_name_ar, 'description': description})
            
            dept_id = result.fetchone()[0]
            conn.commit()
            return dept_id
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_dashboard_stats(self, user_id: int = None) -> Dict:
        """إحصائيات Dashboard"""
        conn = self.get_connection()
        
        stats = {}
        
        if user_id:
            result = conn.execute(text("SELECT COUNT(*) FROM service_requests WHERE created_by = :id"), {'id': user_id}).fetchone()
            stats['total_requests'] = result[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM service_requests WHERE created_by = :id AND status = 'pending'"), {'id': user_id}).fetchone()
            stats['pending_requests'] = result[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM service_requests WHERE created_by = :id AND status = 'in_progress'"), {'id': user_id}).fetchone()
            stats['in_progress_requests'] = result[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM service_requests WHERE created_by = :id AND status = 'completed'"), {'id': user_id}).fetchone()
            stats['completed_requests'] = result[0]
        else:
            result = conn.execute(text("SELECT COUNT(*) FROM service_requests")).fetchone()
            stats['total_requests'] = result[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM service_requests WHERE status = 'pending'")).fetchone()
            stats['pending_requests'] = result[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM service_requests WHERE status = 'in_progress'")).fetchone()
            stats['in_progress_requests'] = result[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM service_requests WHERE status = 'completed'")).fetchone()
            stats['completed_requests'] = result[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM branches WHERE is_active = 1")).fetchone()
            stats['total_branches'] = result[0]
            
            result = conn.execute(text("SELECT COUNT(*) FROM users WHERE is_active = 1")).fetchone()
            stats['total_users'] = result[0]
        
        conn.close()
        return stats
