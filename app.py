"""
Enterprise Workflow System - v4.0
نظام الحوكمة المتكامل - مع الصلاحيات والإشعارات
"""

from flask import Flask, render_template, request, redirect, url_for, flash, send_file, jsonify
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from database import Database
from functools import wraps
import os
import io
import shutil
import sqlite3
from datetime import datetime

app = Flask(__name__)

@app.after_request
def remove_target_blank(response):
    """إزالة target="_blank" من كل الروابط"""
    if response.content_type and 'text/html' in response.content_type:
        response.set_data(response.get_data().replace(b'target="_blank"', b''))
    return response
app.secret_key = os.urandom(24)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

db = Database()

# ✅ إضافة عمود can_backup إذا ما موجود
def migrate_database():
    try:
        conn = sqlite3.connect(db.db_path)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(user_permissions)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'can_backup' not in columns:
            cursor.execute("ALTER TABLE user_permissions ADD COLUMN can_backup INTEGER DEFAULT 0")
            conn.commit()
            print("✅ Added can_backup column")
        else:
            print("✅ can_backup column already exists")
        conn.close()
    except Exception as e:
        print(f"Migration info: {e}")

migrate_database()

class User(UserMixin):
    def __init__(self, user_data):
        self.id = user_data['id']
        self.username = user_data['username']
        self.full_name = user_data['full_name']
        self.email = user_data.get('email', '')
        self.role = user_data['role']
        self.department = user_data['department']
        self.branch_id = user_data.get('branch_id')
        self.permissions = db.get_user_permissions(user_data['id'])

@login_manager.user_loader
def load_user(user_id):
    user_data = db.get_user_by_id(int(user_id))
    if user_data:
        return User(user_data)
    return None

def permission_required(permission_name):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('login'))
            if current_user.role == 'compliance_officer':
                return f(*args, **kwargs)
            if not current_user.permissions.get(permission_name, 0):
                flash('ليس لديك صلاحية للوصول لهذه الصفحة', 'danger')
                return redirect(url_for('dashboard'))
            return f(*args, **kwargs)
        return decorated_function
    return decorator

@app.route('/')
def index():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user_data = db.verify_login(username, password)
        if user_data:
            user = User(user_data)
            login_user(user)
            flash(f'مرحباً {user.full_name}!', 'success')
            return redirect(url_for('dashboard'))
        else:
            flash('اسم المستخدم أو كلمة المرور غير صحيحة', 'danger')
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('تم تسجيل الخروج بنجاح', 'success')
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    if current_user.role in ['compliance_officer', 'general_manager']:
        stats = db.get_dashboard_stats()
    else:
        stats = db.get_dashboard_stats(current_user.id)
    if current_user.permissions.get('can_view_requests', 0) or current_user.role in ['compliance_officer', 'general_manager', 'department_head']:
        if current_user.role in ['compliance_officer', 'general_manager', 'department_head']:
            recent_requests = db.get_all_requests()[:5]
        else:
            recent_requests = db.get_requests_by_user(current_user.id)[:5]
    else:
        recent_requests = []
    unread_count = db.get_unread_count(current_user.id)
    return render_template('dashboard.html', user=current_user, stats=stats, recent_requests=recent_requests, unread_count=unread_count)

# ==================== الفروع ====================
@app.route('/branches/manage')
@login_required
@permission_required('can_manage_branches')
def manage_branches():
    branches_list = db.get_all_branches(include_inactive=True)
    return render_template('manage_branches.html', user=current_user, branches=branches_list)

@app.route('/branches/add', methods=['POST'])
@login_required
@permission_required('can_manage_branches')
def add_branch():
    data = {
        'name': request.form.get('name'),
        'code': request.form.get('code'),
        'location': request.form.get('location'),
        'manager_name': request.form.get('manager_name'),
        'contact_phone': request.form.get('contact_phone'),
        'is_active': 1
    }
    try:
        db.create_branch(data)
        flash('تم إضافة الفرع بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ في إضافة الفرع: {str(e)}', 'danger')
    return redirect(url_for('manage_branches'))

@app.route('/branches/edit/<int:branch_id>', methods=['POST'])
@login_required
@permission_required('can_manage_branches')
def edit_branch(branch_id):
    data = {
        'name': request.form.get('name'),
        'code': request.form.get('code'),
        'location': request.form.get('location'),
        'manager_name': request.form.get('manager_name'),
        'contact_phone': request.form.get('contact_phone'),
        'is_active': int(request.form.get('is_active', 1))
    }
    try:
        db.update_branch(branch_id, data)
        flash('تم تحديث الفرع بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ في تحديث الفرع: {str(e)}', 'danger')
    return redirect(url_for('manage_branches'))

@app.route('/branches/toggle/<int:branch_id>')
@login_required
@permission_required('can_manage_branches')
def toggle_branch(branch_id):
    try:
        db.toggle_branch_status(branch_id)
        flash('تم تغيير حالة الفرع بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('manage_branches'))

# ✅ جديد: حذف الفرع نهائياً
@app.route('/branches/delete/<int:branch_id>')
@login_required
@permission_required('can_manage_branches')
def delete_branch(branch_id):
    try:
        db.delete_branch(branch_id)
        flash('تم حذف الفرع بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ في حذف الفرع: {str(e)}', 'danger')
    return redirect(url_for('manage_branches'))

# ==================== المستخدمين ====================
@app.route('/users/manage')
@login_required
@permission_required('can_manage_users')
def manage_users():
    users_list = db.get_all_users(include_inactive=True)
    branches_list = db.get_all_branches()
    roles_list = db.get_all_roles()
    departments_list = db.get_all_departments()
    return render_template('manage_users.html', user=current_user, users=users_list, branches=branches_list, roles=roles_list, departments=departments_list)

@app.route('/users/add', methods=['POST'])
@login_required
@permission_required('can_manage_users')
def add_user():
    data = {
        'username': request.form.get('username'),
        'password': request.form.get('password'),
        'full_name': request.form.get('full_name'),
        'email': request.form.get('email'),
        'role': request.form.get('role'),
        'department': request.form.get('department'),
        'branch_id': request.form.get('branch_id') or None,
        'is_active': 1
    }
    try:
        db.create_user(data)
        flash('تم إضافة المستخدم بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ في إضافة المستخدم: {str(e)}', 'danger')
    return redirect(url_for('manage_users'))

@app.route('/users/edit/<int:user_id>', methods=['POST'])
@login_required
@permission_required('can_manage_users')
def edit_user(user_id):
    data = {
        'full_name': request.form.get('full_name'),
        'email': request.form.get('email'),
        'role': request.form.get('role'),
        'department': request.form.get('department'),
        'branch_id': request.form.get('branch_id') or None,
        'is_active': int(request.form.get('is_active', 1))
    }
    new_password = request.form.get('new_password')
    if new_password:
        data['password'] = new_password
    try:
        db.update_user(user_id, data)
        flash('تم تحديث المستخدم بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ في تحديث المستخدم: {str(e)}', 'danger')
    return redirect(url_for('manage_users'))

@app.route('/users/toggle/<int:user_id>')
@login_required
@permission_required('can_manage_users')
def toggle_user(user_id):
    try:
        db.toggle_user_status(user_id)
        flash('تم تغيير حالة المستخدم بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('manage_users'))

@app.route('/users/delete/<int:user_id>')
@login_required
@permission_required('can_manage_users')
def delete_user(user_id):
    if user_id == current_user.id:
        flash('لا يمكنك حذف حسابك الخاص!', 'danger')
        return redirect(url_for('manage_users'))
    try:
        db.delete_user(user_id)
        flash('تم حذف المستخدم بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ في حذف المستخدم: {str(e)}', 'danger')
    return redirect(url_for('manage_users'))

@app.route('/users/export_excel')
@login_required
@permission_required('can_manage_users')
def export_users_excel():
    try:
        import xlsxwriter
        users_list = db.get_all_users(include_inactive=True)
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet('المستخدمين')
        header_format = workbook.add_format({'bold': True, 'bg_color': '#1a237e', 'font_color': 'white', 'align': 'center'})
        headers = ['#', 'اسم المستخدم', 'الاسم الكامل', 'البريد', 'الدور', 'القسم', 'الفرع', 'الحالة', 'تاريخ الإنشاء']
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        for row, user in enumerate(users_list, start=1):
            worksheet.write(row, 0, user['id'])
            worksheet.write(row, 1, user['username'])
            worksheet.write(row, 2, user['full_name'])
            worksheet.write(row, 3, user.get('email', ''))
            role_ar = ''
            if user['role'] == 'compliance_officer':
                role_ar = 'مسؤول الامتثال'
            elif user['role'] == 'general_manager':
                role_ar = 'مدير عام'
            elif user['role'] == 'department_head':
                role_ar = 'رئيس قسم'
            else:
                role_ar = 'موظف'
            worksheet.write(row, 4, role_ar)
            worksheet.write(row, 5, user.get('department', ''))
            worksheet.write(row, 6, user.get('branch_name', ''))
            worksheet.write(row, 7, 'نشط' if user['is_active'] else 'معطل')
            worksheet.write(row, 8, user['created_at'][:10])
        worksheet.set_column(0, 0, 8)
        worksheet.set_column(1, 1, 20)
        worksheet.set_column(2, 2, 25)
        worksheet.set_column(3, 3, 30)
        worksheet.set_column(4, 8, 15)
        workbook.close()
        output.seek(0)
        filename = f'users_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=filename)
    except Exception as e:
        flash(f'خطأ في تصدير Excel: {str(e)}', 'danger')
        return redirect(url_for('manage_users'))

# ==================== الطلبات ====================
@app.route('/requests')
@login_required
def requests():
    if not current_user.permissions.get('can_view_requests', 0) and current_user.role not in ['compliance_officer', 'general_manager', 'department_head']:
        flash('ليس لديك صلاحية لعرض الطلبات', 'danger')
        return redirect(url_for('dashboard'))
    if current_user.role in ['compliance_officer', 'general_manager', 'department_head']:
        requests_list = db.get_all_requests()
    else:
        requests_list = db.get_requests_by_user(current_user.id)
    statuses = db.get_all_statuses()
    request_types = db.get_all_request_types()
    return render_template('requests.html', user=current_user, requests=requests_list, statuses=statuses, request_types=request_types)

@app.route('/requests/view/<int:request_id>')
@login_required
def view_request(request_id):
    req = db.get_request_by_id(request_id)
    if not req:
        flash('الطلب غير موجود', 'danger')
        return redirect(url_for('requests'))
    statuses = db.get_all_statuses()
    return render_template('view_request.html', user=current_user, request=req, statuses=statuses)

@app.route('/requests/update_status/<int:request_id>', methods=['POST'])
@login_required
def update_request_status(request_id):
    if current_user.role not in ['compliance_officer', 'general_manager', 'department_head']:
        flash('ليس لديك صلاحية', 'danger')
        return redirect(url_for('requests'))
    status = request.form.get('status')
    notes = request.form.get('notes', '')
    try:
        db.update_request_status(request_id, status, notes)
        flash('تم تحديث حالة الطلب بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('view_request', request_id=request_id))

@app.route('/requests/delete/<int:request_id>', methods=['GET', 'POST'])
@login_required
def delete_request(request_id):
    req = db.get_request_by_id(request_id)
    if not req:
        flash('الطلب غير موجود', 'danger')
        return redirect(url_for('requests'))
    
    # التحقق من الصلاحية: compliance_officer أو general_manager أو صاحب الطلب
    if current_user.role not in ['compliance_officer', 'general_manager'] and req['created_by'] != current_user.id:
        flash('ليس لديك صلاحية لحذف هذا الطلب', 'danger')
        return redirect(url_for('requests'))
    
    try:
        db.delete_request(request_id)
        flash('تم حذف الطلب بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ في حذف الطلب: {str(e)}', 'danger')
    
    return redirect(url_for('requests'))

@app.route('/requests/new', methods=['GET', 'POST'])
@login_required
def new_request():
    if request.method == 'POST':
        data = {
            'request_type': request.form.get('request_type'),
            'title': request.form.get('title'),
            'description': request.form.get('description'),
            'priority': request.form.get('priority', 'medium'),
            'created_by': current_user.id,
            'department': request.form.get('department'),  # ✅ تغيّر من current_user.department
            'branch_id': current_user.branch_id
        }

        request_id = db.create_request(data)

        managers = db.get_all_users()
        for manager in managers:
            if manager['role'] in ['compliance_officer', 'general_manager', 'department_head']:
                db.create_notification(
                    manager['id'],
                    request_id,
                    f"طلب جديد #{request_id} من {current_user.full_name}"
                )

        flash(f'تم إنشاء الطلب #{request_id} بنجاح', 'success')
        return redirect(url_for('requests'))

    request_types = db.get_all_request_types()
    departments = db.get_all_departments()  # ✅ جديد
    return render_template('new_request.html', user=current_user, request_types=request_types, departments=departments)

# ==================== متغيرات النظام ====================
@app.route('/system/variables')
@login_required
@permission_required('can_manage_system_vars')
def system_variables():
    roles = db.get_all_roles()
    departments = db.get_all_departments()
    statuses = db.get_all_statuses()
    request_types = db.get_all_request_types()
    return render_template('system_variables.html', user=current_user, roles=roles, departments=departments, statuses=statuses, request_types=request_types)

# --- الأدوار ---
@app.route('/system/add_role', methods=['POST'])
@login_required
@permission_required('can_manage_system_vars')
def add_role():
    try:
        db.add_role(request.form.get('role_name'), request.form.get('role_name_ar'), request.form.get('description', ''))
        flash('تم إضافة الدور بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

@app.route('/system/edit_role/<int:role_id>', methods=['POST'])
@login_required
@permission_required('can_manage_system_vars')
def edit_role(role_id):
    try:
        db.update_role(role_id, request.form.get('role_name'), request.form.get('role_name_ar'), request.form.get('description', ''))
        flash('تم تحديث الدور بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

@app.route('/system/delete_role/<int:role_id>')
@login_required
@permission_required('can_manage_system_vars')
def delete_role(role_id):
    try:
        db.delete_role(role_id)
        flash('تم حذف الدور بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

# --- الأقسام ---
@app.route('/system/add_department', methods=['POST'])
@login_required
@permission_required('can_manage_system_vars')
def add_department():
    try:
        db.add_department(request.form.get('dept_name'), request.form.get('dept_name_ar'), request.form.get('description', ''))
        flash('تم إضافة القسم بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

@app.route('/system/edit_department/<int:dept_id>', methods=['POST'])
@login_required
@permission_required('can_manage_system_vars')
def edit_department(dept_id):
    try:
        db.update_department(dept_id, request.form.get('dept_name'), request.form.get('dept_name_ar'), request.form.get('description', ''))
        flash('تم تحديث القسم بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

@app.route('/system/delete_department/<int:dept_id>')
@login_required
@permission_required('can_manage_system_vars')
def delete_department(dept_id):
    try:
        db.delete_department(dept_id)
        flash('تم حذف القسم بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

# --- الحالات ---
@app.route('/system/add_status', methods=['POST'])
@login_required
@permission_required('can_manage_system_vars')
def add_status():
    try:
        db.add_status(request.form.get('status_name'), request.form.get('status_name_ar'), request.form.get('status_color', '#000000'))
        flash('تم إضافة الحالة بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

@app.route('/system/edit_status/<int:status_id>', methods=['POST'])
@login_required
@permission_required('can_manage_system_vars')
def edit_status(status_id):
    try:
        db.update_status(status_id, request.form.get('status_name'), request.form.get('status_name_ar'), request.form.get('status_color', '#000000'))
        flash('تم تحديث الحالة بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

@app.route('/system/delete_status/<int:status_id>')
@login_required
@permission_required('can_manage_system_vars')
def delete_status(status_id):
    try:
        db.delete_status(status_id)
        flash('تم حذف الحالة بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

# ✅ جديد: أنواع الطلبات
@app.route('/system/add_request_type', methods=['POST'])
@login_required
@permission_required('can_manage_system_vars')
def add_request_type():
    try:
        db.add_request_type(request.form.get('type_name'), request.form.get('type_name_ar'), request.form.get('description', ''))
        flash('تم إضافة نوع الطلب بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

@app.route('/system/edit_request_type/<int:type_id>', methods=['POST'])
@login_required
@permission_required('can_manage_system_vars')
def edit_request_type(type_id):
    try:
        db.update_request_type(type_id, request.form.get('type_name'), request.form.get('type_name_ar'), request.form.get('description', ''))
        flash('تم تحديث نوع الطلب بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

@app.route('/system/delete_request_type/<int:type_id>')
@login_required
@permission_required('can_manage_system_vars')
def delete_request_type(type_id):
    try:
        db.delete_request_type(type_id)
        flash('تم حذف نوع الطلب بنجاح', 'success')
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
    return redirect(url_for('system_variables'))

# ==================== الصلاحيات ====================
@app.route('/permissions/manage')
@login_required
def manage_permissions():
    if current_user.role != 'compliance_officer':
        flash('ليس لديك صلاحية للوصول لهذه الصفحة', 'danger')
        return redirect(url_for('dashboard'))
    users_list = db.get_all_users()
    return render_template('manage_permissions.html', user=current_user, users=users_list)

@app.route('/permissions/get/<int:user_id>')
@login_required
def get_user_permissions_api(user_id):
    if current_user.role != 'compliance_officer':
        return jsonify({'error': 'Unauthorized'}), 403
    permissions = db.get_user_permissions(user_id)
    return jsonify(permissions)

@app.route('/permissions/set/<int:user_id>', methods=['POST'])
@login_required
def set_user_permissions_api(user_id):
    if current_user.role != 'compliance_officer':
        return jsonify({'success': False, 'message': 'ليس لديك صلاحية'}), 403
    try:
        permissions = {
            'can_manage_users': 1 if request.form.get('can_manage_users') == '1' else 0,
            'can_manage_branches': 1 if request.form.get('can_manage_branches') == '1' else 0,
            'can_manage_system_vars': 1 if request.form.get('can_manage_system_vars') == '1' else 0,
            'can_view_reports': 1 if request.form.get('can_view_reports') == '1' else 0,
            'can_view_requests': 1 if request.form.get('can_view_requests') == '1' else 0,
            'can_backup': 1 if request.form.get('can_backup') == '1' else 0,
        }
        db.set_user_permissions(user_id, permissions)
        return jsonify({'success': True, 'message': 'تم تحديث الصلاحيات بنجاح'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'خطأ: {str(e)}'}), 500

# ==================== الإشعارات ====================
@app.route('/api/notifications/unread')
@login_required
def get_unread_notifications():
    notifications = db.get_unread_notifications(current_user.id)
    count = db.get_unread_count(current_user.id)
    return jsonify({'count': count, 'notifications': notifications})

@app.route('/api/notifications/mark_read/<int:notif_id>', methods=['POST'])
@login_required
def mark_notification_read_api(notif_id):
    db.mark_notification_read(notif_id)
    return jsonify({'success': True})

# ==================== التقارير ====================
@app.route('/reports')
@login_required
@permission_required('can_view_reports')
def reports():
    stats = db.get_dashboard_stats()
    all_requests = db.get_all_requests()
    return render_template('reports.html', user=current_user, stats=stats, requests=all_requests)

@app.route('/reports/export_excel')
@login_required
@permission_required('can_view_reports')
def export_excel():
    try:
        import xlsxwriter
        all_requests = db.get_all_requests()
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output)
        worksheet = workbook.add_worksheet('الطلبات')
        header_format = workbook.add_format({'bold': True, 'bg_color': '#1a237e', 'font_color': 'white', 'align': 'center'})
        headers = ['#', 'العنوان', 'النوع', 'الحالة', 'الأولوية', 'القسم', 'الفرع', 'المنشئ', 'التاريخ']
        for col, header in enumerate(headers):
            worksheet.write(0, col, header, header_format)
        for row, req in enumerate(all_requests, start=1):
            worksheet.write(row, 0, req['id'])
            worksheet.write(row, 1, req['title'])
            worksheet.write(row, 2, req['request_type'])
            worksheet.write(row, 3, req['status'])
            worksheet.write(row, 4, req['priority'])
            worksheet.write(row, 5, req.get('department', ''))
            worksheet.write(row, 6, req.get('branch_name', ''))
            worksheet.write(row, 7, req['creator_name'])
            worksheet.write(row, 8, req['created_at'][:10])
        worksheet.set_column(0, 0, 8)
        worksheet.set_column(1, 1, 30)
        worksheet.set_column(2, 8, 15)
        workbook.close()
        output.seek(0)
        filename = f'requests_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name=filename)
    except Exception as e:
        flash(f'خطأ في تصدير Excel: {str(e)}', 'danger')
        return redirect(url_for('reports'))

# ==================== النسخ الاحتياطي ====================
@app.route('/backup/download')
@login_required
@permission_required('can_backup')
def download_backup():
    try:
        db_path = db.db_path
        if not os.path.exists(db_path):
            flash('قاعدة البيانات غير موجودة', 'danger')
            return redirect(url_for('settings'))
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f'workflow_backup_{timestamp}.db'
        return send_file(db_path, mimetype='application/x-sqlite3', as_attachment=True, download_name=filename)
    except Exception as e:
        flash(f'خطأ في تحميل النسخة الاحتياطية: {str(e)}', 'danger')
        return redirect(url_for('settings'))

@app.route('/backup/upload', methods=['POST'])
@login_required
@permission_required('can_backup')
def upload_backup():
    try:
        if 'backup_file' not in request.files:
            flash('لم يتم اختيار ملف', 'danger')
            return redirect(url_for('settings'))
        file = request.files['backup_file']
        if file.filename == '':
            flash('لم يتم اختيار ملف', 'danger')
            return redirect(url_for('settings'))
        if not file.filename.endswith('.db'):
            flash('الملف يجب أن يكون .db', 'danger')
            return redirect(url_for('settings'))
        upload_path = 'temp_backup.db'
        file.save(upload_path)
        if db.restore_database(upload_path):
            os.remove(upload_path)
            flash('تم استرجاع البيانات بنجاح! الرجاء تسجيل الدخول من جديد', 'success')
            logout_user()
        else:
            flash('خطأ في استرجاع البيانات', 'danger')
        return redirect(url_for('login'))
    except Exception as e:
        flash(f'خطأ: {str(e)}', 'danger')
        return redirect(url_for('settings'))

@app.route('/settings')
@login_required
def settings():
    return render_template('settings.html', user=current_user)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
