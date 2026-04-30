"""
إدارة قاعدة البيانات - PostgreSQL (Supabase)
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import bcrypt

class Database:
    """إدارة قاعدة البيانات"""
    
    def __init__(self):
        # الاتصال بـ Supabase PostgreSQL
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            raise Exception("DATABASE_URL not found in environment variables!")
        
        self.engine = create_engine(database_url, poolclass=NullPool)
        self.init_database()
    
    def get_connection(self):
        """إنشاء اتصال جديد"""
        return self.engine.connect()
    
    def init_database(self):
        """إنشاء الجداول والبيانات الأساسية"""
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
                    department VARCHAR(100),
                    branch_id INTEGER,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # جدول الفروع
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS branches (
                    id SERIAL PRIMARY KEY,
                    code VARCHAR(20) UNIQUE NOT NULL,
                    name VARCHAR(200) NOT NULL,
                    city VARCHAR(100) NOT NULL,
                    address VARCHAR(300),
                    phone VARCHAR(50),
                    manager_name VARCHAR(200),
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # جدول الطلبات
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS requests (
                    id SERIAL PRIMARY KEY,
                    title VARCHAR(300) NOT NULL,
                    description TEXT,
                    request_type VARCHAR(100) NOT NULL,
                    priority VARCHAR(50) DEFAULT 'medium',
                    status VARCHAR(50) DEFAULT 'pending',
                    created_by INTEGER NOT NULL,
                    assigned_to INTEGER,
                    branch_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES users(id),
                    FOREIGN KEY (assigned_to) REFERENCES users(id),
                    FOREIGN KEY (branch_id) REFERENCES branches(id)
                )
            """))
            
            # جدول التعليقات
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS comments (
                    id SERIAL PRIMARY KEY,
                    request_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    comment TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (request_id) REFERENCES requests(id),
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )
            """))
            
            # جدول الصلاحيات
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_permissions (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER UNIQUE NOT NULL,
                    can_manage_users BOOLEAN DEFAULT FALSE,
                    can_manage_branches BOOLEAN DEFAULT FALSE,
                    can_view_requests BOOLEAN DEFAULT FALSE,
                    can_view_reports BOOLEAN DEFAULT FALSE,
                    can_manage_system_vars BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )
            """))
            
            # جدول الإشعارات
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    request_id INTEGER,
                    message TEXT NOT NULL,
                    is_read BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(id),
                    FOREIGN KEY (request_id) REFERENCES requests(id)
                )
            """))
            
            conn.commit()
            
            # إضافة البيانات الأساسية
            self.create_default_data(conn)
            
        except Exception as e:
            conn.rollback()
            print(f"Error initializing database: {e}")
        finally:
            conn.close()
    
    def create_default_data(self, conn):
        """إنشاء البيانات الأساسية"""
        
        # التحقق من وجود مستخدمين
        result = conn.execute(text("SELECT COUNT(*) as count FROM users")).fetchone()
        
        if result[0] == 0:
            # إضافة المستخدمين الافتراضيين
            default_users = [
                ('compliance', 'compliance123', 'مسؤول الامتثال', 'compliance@hawkama.iq', 'compliance_officer', 'Compliance'),
                ('gm', 'gm123', 'المدير العام', 'gm@hawkama.iq', 'general_manager', 'Management'),
                ('it_manager', 'it123', 'مدير تقنية المعلومات', 'it@hawkama.iq', 'department_head', 'IT')
            ]
            
            for username, password, full_name, email, role, dept in default_users:
                hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
                conn.execute(text("""
                    INSERT INTO users (username, password, full_name, email, role, department, is_active)
                    VALUES (:username, :password, :full_name, :email, :role, :department, TRUE)
                """), {
                    'username': username,
                    'password': hashed.decode('utf-8'),
                    'full_name': full_name,
                    'email': email,
                    'role': role,
                    'department': dept
                })
            
            conn.commit()
        
        # التحقق من وجود فروع
        result = conn.execute(text("SELECT COUNT(*) as count FROM branches")).fetchone()
        
        if result[0] == 0:
            # إضافة الفروع الافتراضية
            branches = [
                ('HQ-001', 'المقر الرئيسي - HQ', 'بغداد - المنصور', '07700000001', 'إدارة المقر'),
                ('SVD-001', 'فرع السيدونية', 'بغداد - السيعودن', '07700000002', 'مدير السيعودن'),
                ('HAR-001', 'فرع الحارثية', 'بغداد - الحارثية', '07700000003', 'مدير الحارثية'),
                # ... باقي الفروع
            ]
            
            for code, name, city, phone, manager in branches:
                conn.execute(text("""
                    INSERT INTO branches (code, name, city, phone, manager_name, is_active)
                    VALUES (:code, :name, :city, :phone, :manager, TRUE)
                """), {
                    'code': code,
                    'name': name,
                    'city': city,
                    'phone': phone,
                    'manager': manager
                })
            
            conn.commit()
    
    def get_user_by_username(self, username):
        """الحصول على مستخدم"""
        conn = self.get_connection()
        result = conn.execute(
            text("SELECT * FROM users WHERE username = :username"),
            {'username': username}
        ).fetchone()
        conn.close()
        
        if result:
            return dict(result._mapping)
        return None
    
    def get_user_by_id(self, user_id):
        """الحصول على مستخدم بالمعرف"""
        conn = self.get_connection()
        result = conn.execute(
            text("SELECT * FROM users WHERE id = :id"),
            {'id': user_id}
        ).fetchone()
        conn.close()
        
        if result:
            return dict(result._mapping)
        return None
    
    def verify_password(self, plain_password, hashed_password):
        """التحقق من كلمة المرور"""
        return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    
    def get_all_branches(self, include_inactive=False):
        """الحصول على كل الفروع"""
        conn = self.get_connection()
        
        if include_inactive:
            query = "SELECT * FROM branches ORDER BY name"
            result = conn.execute(text(query)).fetchall()
        else:
            query = "SELECT * FROM branches WHERE is_active = TRUE ORDER BY name"
            result = conn.execute(text(query)).fetchall()
        
        conn.close()
        return [dict(row._mapping) for row in result]
    
    def toggle_branch_status(self, branch_id):
        """تبديل حالة الفرع"""
        conn = self.get_connection()
        conn.execute(
            text("UPDATE branches SET is_active = NOT is_active WHERE id = :id"),
            {'id': branch_id}
        )
        conn.commit()
        conn.close()
        return True
    
    def get_all_users(self, include_inactive=False):
        """الحصول على كل المستخدمين"""
        conn = self.get_connection()
        
        query = """
            SELECT u.*, b.name as branch_name 
            FROM users u 
            LEFT JOIN branches b ON u.branch_id = b.id
        """
        
        if not include_inactive:
            query += " WHERE u.is_active = TRUE"
        
        query += " ORDER BY u.full_name"
        
        result = conn.execute(text(query)).fetchall()
        conn.close()
        
        return [dict(row._mapping) for row in result]
    
    def toggle_user_status(self, user_id):
        """تبديل حالة المستخدم"""
        conn = self.get_connection()
        conn.execute(
            text("UPDATE users SET is_active = NOT is_active WHERE id = :id"),
            {'id': user_id}
        )
        conn.commit()
        conn.close()
        return True
    
    def delete_user(self, user_id):
        """حذف مستخدم نهائياً"""
        conn = self.get_connection()
        
        try:
            # حذف صلاحيات المستخدم
            conn.execute(text("DELETE FROM user_permissions WHERE user_id = :id"), {'id': user_id})
            
            # حذف إشعارات المستخدم
            conn.execute(text("DELETE FROM notifications WHERE user_id = :id"), {'id': user_id})
            
            # حذف المستخدم
            conn.execute(text("DELETE FROM users WHERE id = :id"), {'id': user_id})
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
