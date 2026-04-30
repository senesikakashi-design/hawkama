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
        database_url = os.environ.get('DATABASE_URL')
        if not database_url:
            raise Exception("DATABASE_URL not found!")
        
        self.engine = create_engine(database_url, poolclass=NullPool)
        self.init_database()
    
    def get_connection(self):
        return self.engine.connect()
    
    def _row_to_dict(self, row):
        """تحويل Row إلى dict"""
        if row is None:
            return None
        return {key: value for key, value in row._mapping.items()}
    
    def init_database(self):
        """إنشاء الجداول"""
        conn = self.get_connection()
        
        try:
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
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS comments (
                    id SERIAL PRIMARY KEY,
                    request_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    comment TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_permissions (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER UNIQUE NOT NULL,
                    can_manage_users BOOLEAN DEFAULT FALSE,
                    can_manage_branches BOOLEAN DEFAULT FALSE,
                    can_view_requests BOOLEAN DEFAULT FALSE,
                    can_view_reports BOOLEAN DEFAULT FALSE,
                    can_manage_system_vars BOOLEAN DEFAULT FALSE
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    request_id INTEGER,
                    message TEXT NOT NULL,
                    is_read BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.commit()
            self.create_default_data(conn)
            
        except Exception as e:
            conn.rollback()
            print(f"Database Error: {e}")
        finally:
            conn.close()
    
    def create_default_data(self, conn):
        """البيانات الأساسية"""
        result = conn.execute(text("SELECT COUNT(*) as count FROM users")).fetchone()
        
        if result[0] == 0:
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
        
        result = conn.execute(text("SELECT COUNT(*) as count FROM branches")).fetchone()
        
        if result[0] == 0:
            branches = [
                ('HQ-001', 'المقر الرئيسي - HQ', 'بغداد - المنصور', '07700000001', 'إدارة المقر'),
                ('SVD-001', 'فرع السيعودن', 'بغداد - السيعودن', '07700000002', 'مدير السيعودن'),
                ('HAR-001', 'فرع الحارثية', 'بغداد - الحارثية', '07700000003', 'مدير الحارثية'),
                ('DRA-001', 'فرع الدورة', 'بغداد - الدورة', '07700000006', 'مدير الدورة'),
                ('BSR-001', 'فرع البصرة', 'البصرة - العشار', '07700000009', 'مدير البصرة'),
                ('ERB-001', 'فرع أربيل', 'أربيل', '07700000011', 'مدير أربيل'),
                ('SLM-001', 'فرع السليمانية', 'السليمانية', '07700000016', 'مدير السليمانية')
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
        return self._row_to_dict(result)
    
    def get_user_by_id(self, user_id):
        """الحصول على مستخدم بالمعرف"""
        conn = self.get_connection()
        result = conn.execute(
            text("SELECT * FROM users WHERE id = :id"),
            {'id': user_id}
        ).fetchone()
        conn.close()
        return self._row_to_dict(result)
    
    def verify_password(self, plain_password, hashed_password):
        """التحقق من كلمة المرور"""
        return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    
    def verify_login(self, username, password):
        """التحقق من تسجيل الدخول"""
        user = self.get_user_by_username(username)
        
        if user and user['is_active']:
            if self.verify_password(password, user['password']):
                return user
        
        return None
    
    def get_all_branches(self, include_inactive=False):
        """الحصول على كل الفروع"""
        conn = self.get_connection()
        
        if include_inactive:
            query = "SELECT * FROM branches ORDER BY name"
        else:
            query = "SELECT * FROM branches WHERE is_active = TRUE ORDER BY name"
        
        result = conn.execute(text(query)).fetchall()
        conn.close()
        return [self._row_to_dict(row) for row in result]
    
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
        return [self._row_to_dict(row) for row in result]
    
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
    
    def get_user_permissions(self, user_id):
        """الحصول على صلاحيات المستخدم"""
        conn = self.get_connection()
        result = conn.execute(
            text("SELECT * FROM user_permissions WHERE user_id = :id"),
            {'id': user_id}
        ).fetchone()
        conn.close()
        
        if result:
            return self._row_to_dict(result)
        
        return {
            'can_manage_users': False,
            'can_manage_branches': False,
            'can_view_requests': False,
            'can_view_reports': False,
            'can_manage_system_vars': False
        }
    
    def add_user(self, data):
        """إضافة مستخدم جديد"""
        conn = self.get_connection()
        
        try:
            hashed = bcrypt.hashpw(data['password'].encode('utf-8'), bcrypt.gensalt())
            
            conn.execute(text("""
                INSERT INTO users (username, password, full_name, email, role, department, branch_id, is_active)
                VALUES (:username, :password, :full_name, :email, :role, :department, :branch_id, TRUE)
            """), {
                'username': data['username'],
                'password': hashed.decode('utf-8'),
                'full_name': data['full_name'],
                'email': data.get('email', ''),
                'role': data['role'],
                'department': data['department'],
                'branch_id': data.get('branch_id')
            })
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def update_user(self, user_id, data):
        """تحديث مستخدم"""
        conn = self.get_connection()
        
        try:
            query = """
                UPDATE users 
                SET full_name = :full_name, 
                    email = :email, 
                    role = :role, 
                    department = :department, 
                    branch_id = :branch_id,
                    is_active = :is_active
            """
            
            params = {
                'full_name': data['full_name'],
                'email': data.get('email', ''),
                'role': data['role'],
                'department': data['department'],
                'branch_id': data.get('branch_id'),
                'is_active': data.get('is_active', True),
                'user_id': user_id
            }
            
            if 'password' in data and data['password']:
                hashed = bcrypt.hashpw(data['password'].encode('utf-8'), bcrypt.gensalt())
                query += ", password = :password"
                params['password'] = hashed.decode('utf-8')
            
            query += " WHERE id = :user_id"
            
            conn.execute(text(query), params)
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_branch_by_id(self, branch_id):
        """الحصول على فرع"""
        conn = self.get_connection()
        result = conn.execute(
            text("SELECT * FROM branches WHERE id = :id"),
            {'id': branch_id}
        ).fetchone()
        conn.close()
        return self._row_to_dict(result)
    
    def update_branch(self, branch_id, data):
        """تحديث فرع"""
        conn = self.get_connection()
        
        try:
            conn.execute(text("""
                UPDATE branches 
                SET name = :name, 
                    city = :city, 
                    address = :address, 
                    phone = :phone, 
                    manager_name = :manager
                WHERE id = :id
            """), {
                'name': data['name'],
                'city': data['city'],
                'address': data.get('address', ''),
                'phone': data.get('phone', ''),
                'manager': data.get('manager_name', ''),
                'id': branch_id
            })
            
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
    
    def get_all_requests(self, user=None, status=None):
        """الحصول على كل الطلبات"""
        conn = self.get_connection()
        
        query = """
            SELECT r.*, 
                   u1.full_name as creator_name,
                   u2.full_name as assignee_name,
                   b.name as branch_name
            FROM requests r
            LEFT JOIN users u1 ON r.created_by = u1.id
            LEFT JOIN users u2 ON r.assigned_to = u2.id
            LEFT JOIN branches b ON r.branch_id = b.id
            ORDER BY r.created_at DESC
        """
        
        result = conn.execute(text(query)).fetchall()
        conn.close()
        return [self._row_to_dict(row) for row in result]
    
    def create_backup(self):
        """إنشاء نسخة احتياطية"""
        return True
