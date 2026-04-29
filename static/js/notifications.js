// نظام الإشعارات
let lastNotificationCount = 0;

// التحقق من الإشعارات كل 30 ثانية
setInterval(checkNotifications, 30000);

// التحقق عند تحميل الصفحة
document.addEventListener('DOMContentLoaded', function() {
    checkNotifications();
});

function checkNotifications() {
    fetch('/api/notifications/unread')
        .then(response => response.json())
        .then(data => {
            const count = data.count;
            const notifications = data.notifications;
            
            // تحديث العداد
            const badge = document.getElementById('notifBadge');
            if (count > 0) {
                badge.textContent = count;
                badge.style.display = 'inline-block';
                
                // إذا زاد العدد - تشغيل الصوت والاهتزاز
                if (count > lastNotificationCount) {
                    playNotificationSound();
                    shakeBell();
                }
            } else {
                badge.style.display = 'none';
            }
            
            lastNotificationCount = count;
            
            // تحديث القائمة
            updateNotificationsList(notifications);
        })
        .catch(error => console.error('خطأ في تحميل الإشعارات:', error));
}

function updateNotificationsList(notifications) {
    const list = document.getElementById('notificationsList');
    
    if (notifications.length === 0) {
        list.innerHTML = '<li><p class="dropdown-item text-muted text-center">لا توجد إشعارات جديدة</p></li>';
        return;
    }
    
    let html = '';
    notifications.forEach(notif => {
        const timeAgo = getTimeAgo(notif.created_at);
        html += `
            <li>
                <a class="dropdown-item" href="#" onclick="goToRequest(${notif.request_id}, ${notif.id}); return false;">
                    <div class="d-flex align-items-start">
                        <i class="fas fa-circle text-primary me-2" style="font-size: 0.5rem; margin-top: 0.5rem;"></i>
                        <div>
                            <strong>${notif.message}</strong>
                            <br>
                            <small class="text-muted">${timeAgo}</small>
                        </div>
                    </div>
                </a>
            </li>
            <li><hr class="dropdown-divider"></li>
        `;
    });
    
    list.innerHTML = html;
}

function goToRequest(requestId, notifId) {
    // وضع علامة مقروء
    fetch(`/api/notifications/mark_read/${notifId}`, {
        method: 'POST'
    }).then(() => {
        // الانتقال للطلب
        window.location.href = `/requests/view/${requestId}`;
    });
}

function playNotificationSound() {
    const sound = document.getElementById('notificationSound');
    if (sound) {
        sound.play().catch(e => console.log('لا يمكن تشغيل الصوت:', e));
    }
}

function shakeBell() {
    const bell = document.getElementById('bellIcon');
    bell.classList.add('bell-shake');
    setTimeout(() => {
        bell.classList.remove('bell-shake');
    }, 500);
}

function getTimeAgo(datetime) {
    const now = new Date();
    const then = new Date(datetime);
    const diff = Math.floor((now - then) / 1000); // بالثواني
    
    if (diff < 60) return 'الآن';
    if (diff < 3600) return `منذ ${Math.floor(diff / 60)} دقيقة`;
    if (diff < 86400) return `منذ ${Math.floor(diff / 3600)} ساعة`;
    return `منذ ${Math.floor(diff / 86400)} يوم`;
}
