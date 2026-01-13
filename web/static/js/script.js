// State
let allUsers = [];
let editingId = null;

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadData();
    
    // Search Filter
    document.getElementById('searchInput').addEventListener('input', (e) => {
        const term = e.target.value.toLowerCase();
        const filtered = allUsers.filter(user => 
            user.full_name.toLowerCase().includes(term) || 
            user.email.toLowerCase().includes(term)
        );
        renderTable(filtered);
    });

    // Form Submit
    document.getElementById('userForm').addEventListener('submit', handleFormSubmit);
    
    // Outside click modal close
    document.getElementById('userModal').addEventListener('click', (e) => {
        if (e.target.id === 'userModal') closeModal();
    });
});

async function loadData() {
    try {
        await Promise.all([fetchUsers(), fetchStats()]);
    } catch (err) {
        showToast('Failed to load data', 'error');
    }
}

async function fetchUsers() {
    const res = await fetch('/api/users');
    allUsers = await res.json();
    renderTable(allUsers);
}

async function fetchStats() {
    const res = await fetch('/api/stats');
    const stats = await res.json();
    
    document.getElementById('totalUsers').textContent = stats.total_users;
    document.getElementById('activeUsers').textContent = stats.active_users;
    document.getElementById('deptCount').textContent = stats.departments;
}

function renderTable(users) {
    const tbody = document.getElementById('userTableBody');
    
    if (users.length === 0) {
        tbody.innerHTML = `<tr><td colspan="6" style="text-align:center; padding: 2rem; color: #6b7280;">No users found. Click "Add New User" to start.</td></tr>`;
        return;
    }

    tbody.innerHTML = users.map(user => `
        <tr>
            <td>
                <div class="user-cell">
                    <div class="user-avatar">${getInitials(user.full_name)}</div>
                    <div>
                        <div style="font-weight: 500;">${user.full_name}</div>
                        <div style="font-size: 0.75rem; color: #6b7280;">${user.email}</div>
                    </div>
                </div>
            </td>
            <td><span class="role-badge">${user.role}</span></td>
            <td>${user.department}</td>
            <td>
                <span class="status-badge ${user.status === 'Active' ? 'status-active' : 'status-inactive'}">
                    ${user.status}
                </span>
            </td>
            <td>${user.joined_date || '-'}</td>
            <td>
                <button class="btn btn-secondary" style="padding: 4px 8px;" onclick="editUser(${user.id})">Edit</button>
                <button class="btn btn-danger" style="padding: 4px 8px; margin-left: 4px;" onclick="deleteUser(${user.id})">Delete</button>
            </td>
        </tr>
    `).join('');
}

function getInitials(name) {
    return name.split(' ').map(n => n[0]).join('').substring(0, 2).toUpperCase();
}

// --- Modal & Form ---

function openModal() {
    editingId = null;
    document.getElementById('modalTitle').textContent = 'Add New User';
    document.getElementById('submitBtn').textContent = 'Create User';
    document.getElementById('userForm').reset();
    document.getElementById('userModal').classList.add('active');
}

function closeModal() {
    document.getElementById('userModal').classList.remove('active');
}

async function editUser(id) {
    try {
        const res = await fetch(`/api/users/${id}`);
        const user = await res.json();
        
        editingId = id;
        document.getElementById('modalTitle').textContent = 'Edit User';
        document.getElementById('submitBtn').textContent = 'Update User';
        
        document.getElementById('fullName').value = user.full_name;
        document.getElementById('email').value = user.email;
        document.getElementById('department').value = user.department;
        document.getElementById('role').value = user.role;
        
        // Handle Radio buttons for status
        const radios = document.getElementsByName('status');
        for (const r of radios) {
            if (r.value === user.status) r.checked = true;
        }

        document.getElementById('userModal').classList.add('active');
    } catch (err) {
        showToast('Error loading user details', 'error');
    }
}

async function handleFormSubmit(e) {
    e.preventDefault();
    
    const formData = {
        full_name: document.getElementById('fullName').value,
        email: document.getElementById('email').value,
        department: document.getElementById('department').value,
        role: document.getElementById('role').value,
        status: document.querySelector('input[name="status"]:checked').value
    };

    const url = editingId ? `/api/users/${editingId}` : '/api/users';
    const method = editingId ? 'PUT' : 'POST';

    try {
        const res = await fetch(url, {
            method: method,
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(formData)
        });

        const data = await res.json();

        if (res.ok) {
            showToast(editingId ? 'User updated successfully' : 'User created successfully', 'success');
            closeModal();
            loadData();
        } else {
            showToast(data.error || 'Operation failed', 'error');
        }
    } catch (err) {
        showToast('Network error occurred', 'error');
    }
}

async function deleteUser(id) {
    if (!confirm('Are you sure you want to delete this user?')) return;
    
    try {
        const res = await fetch(`/api/users/${id}`, { method: 'DELETE' });
        if (res.ok) {
            showToast('User deleted', 'success');
            loadData();
        } else {
            showToast('Failed to delete user', 'error');
        }
    } catch (err) {
        showToast('Error deleting user', 'error');
    }
}

function showToast(msg, type) {
    const toast = document.getElementById('toast');
    toast.textContent = msg;
    toast.className = `toast show ${type}`;
    setTimeout(() => {
        toast.className = 'toast';
    }, 3000);
}