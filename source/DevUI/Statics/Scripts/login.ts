document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const form = document.getElementById('login-form') as HTMLFormElement;
    const usernameInput = document.getElementById('username') as HTMLInputElement;
    const passwordInput = document.getElementById('password') as HTMLInputElement;
    const loginButton = document.getElementById('login-button') as HTMLButtonElement;
    const usernameError = document.getElementById('username-error') as HTMLDivElement;
    const passwordError = document.getElementById('password-error') as HTMLDivElement;
    const loginError = document.getElementById('login-error') as HTMLDivElement;
    
    // Form validation
    function validateForm(): boolean {
        let isValid = true;
        
        // Reset error messages
        usernameError.style.display = 'none';
        passwordError.style.display = 'none';
        loginError.style.display = 'none';
        
        // Validate username
        if (!usernameInput.value.trim()) {
            usernameError.textContent = 'Пожалуйста, введите имя пользователя';
            usernameError.style.display = 'block';
            usernameInput.classList.add('invalid');
            isValid = false;
        } else {
            usernameInput.classList.remove('invalid');
        }
        
        // Validate password
        if (!passwordInput.value) {
            passwordError.textContent = 'Пожалуйста, введите пароль';
            passwordError.style.display = 'block';
            passwordInput.classList.add('invalid');
            isValid = false;
        } else {
            passwordInput.classList.remove('invalid');
        }
        
        return isValid;
    }
    
    // Form submission
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        if (!validateForm()) {
            return;
        }
        
        const userData = {
            username: usernameInput.value,
            password: passwordInput.value
        };
        
        try {
            loginButton.disabled = true;
            loginButton.textContent = 'Вход...';
            
            // Replace with your API endpoint
            const response = await fetch('your api here', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(userData)
            });
            
            if (response.ok) {
                // Login successful - redirect to dashboard or home page
                const data = await response.json();
                
                // Store auth token if provided by the API
                if (data.token) {
                    localStorage.setItem('auth_token', data.token);
                }
                
                // Redirect to main page after successful login
                window.location.href = 'index.html';
            } else {
                // Handle error response
                const errorData = await response.json();
                loginError.textContent = errorData.message || 'Неверное имя пользователя или пароль';
                loginError.style.display = 'block';
            }
        } catch (error) {
            console.error('Login error:', error);
            loginError.textContent = 'Ошибка при отправке запроса. Пожалуйста, попробуйте снова.';
            loginError.style.display = 'block';
        } finally {
            loginButton.disabled = false;
            loginButton.textContent = 'Войти';
        }
    });
    
    // Clear error messages when user starts typing again
    usernameInput.addEventListener('input', () => {
        usernameError.style.display = 'none';
        loginError.style.display = 'none';
        usernameInput.classList.remove('invalid');
    });
    
    passwordInput.addEventListener('input', () => {
        passwordError.style.display = 'none';
        loginError.style.display = 'none';
        passwordInput.classList.remove('invalid');
    });
});
