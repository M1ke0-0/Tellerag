document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const form = document.getElementById('signin-form') as HTMLFormElement;
    const usernameInput = document.getElementById('username') as HTMLInputElement;
    const passwordInput = document.getElementById('password') as HTMLInputElement;
    const confirmPasswordInput = document.getElementById('confirm-password') as HTMLInputElement;
    const requirementsList = document.getElementById('password-requirements') as HTMLDivElement;
    const submitButton = document.getElementById('signin-button') as HTMLButtonElement;
    const passwordMatchError = document.getElementById('password-match-error') as HTMLDivElement;
    
    // Requirement elements
    const reqLength = document.getElementById('req-length') as HTMLLIElement;
    const reqUppercase = document.getElementById('req-uppercase') as HTMLLIElement;
    const reqLowercase = document.getElementById('req-lowercase') as HTMLLIElement;
    const reqUnderscore = document.getElementById('req-underscore') as HTMLLIElement;
    
    // Password validation criteria
    const minLength = 8;
    const hasUppercase = /[A-Z]/;
    const hasLowercase = /[a-z]/;
    const hasUnderscore = /[_]/;
    
    // Password validation state
    interface PasswordValidity {
        length: boolean;
        uppercase: boolean;
        lowercase: boolean;
        underscore: boolean;
    }
    
    let passwordValid = false;
    let passwordsMatch = false;
    
    // Show password requirements when password field is focused
    passwordInput.addEventListener('focus', () => {
        requirementsList.style.display = 'block';
    });
    
    // Validate password as user types
    passwordInput.addEventListener('input', validatePassword);
    
    // Check if passwords match
    confirmPasswordInput.addEventListener('input', checkPasswordsMatch);
    
    // Password validation function
    function validatePassword() {
        const password = passwordInput.value;
        
        // Check each requirement
        const validity: PasswordValidity = {
            length: password.length >= minLength,
            uppercase: hasUppercase.test(password),
            lowercase: hasLowercase.test(password),
            underscore: hasUnderscore.test(password)
        };
        
        // Update UI for each requirement
        updateRequirement(reqLength, validity.length);
        updateRequirement(reqUppercase, validity.uppercase);
        updateRequirement(reqLowercase, validity.lowercase);
        updateRequirement(reqUnderscore, validity.underscore);
        
        // Update password field validation state
        passwordValid = Object.values(validity).every(Boolean);
        
        if (passwordValid) {
            passwordInput.classList.add('valid');
            passwordInput.classList.remove('invalid');
        } else {
            passwordInput.classList.add('invalid');
            passwordInput.classList.remove('valid');
        }
        
        // Check passwords match again (if confirm field has value)
        if (confirmPasswordInput.value) {
            checkPasswordsMatch();
        }
        
        updateSubmitButton();
    }
    
    // Check if passwords match
    function checkPasswordsMatch() {
        const password = passwordInput.value;
        const confirmPassword = confirmPasswordInput.value;
        
        passwordsMatch = password === confirmPassword && confirmPassword !== '';
        
        if (confirmPassword === '') {
            confirmPasswordInput.classList.remove('valid', 'invalid');
            passwordMatchError.style.display = 'none';
        } else if (passwordsMatch) {
            confirmPasswordInput.classList.add('valid');
            confirmPasswordInput.classList.remove('invalid');
            passwordMatchError.style.display = 'none';
        } else {
            confirmPasswordInput.classList.add('invalid');
            confirmPasswordInput.classList.remove('valid');
            passwordMatchError.style.display = 'block';
        }
        
        updateSubmitButton();
    }
    
    // Update a requirement element
    function updateRequirement(element: HTMLElement, valid: boolean) {
        if (valid) {
            element.classList.add('valid');
        } else {
            element.classList.remove('valid');
        }
    }
    
    // Update submit button state
    function updateSubmitButton() {
        submitButton.disabled = !(passwordValid && passwordsMatch && usernameInput.value !== '');
    }
    
    // Form submission
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        if (!passwordValid || !passwordsMatch) {
            return;
        }
        
        const userData = {
            username: usernameInput.value,
            password: passwordInput.value
        };
        
        try {
            submitButton.disabled = true;
            submitButton.textContent = 'Регистрация...';
            
            // Replace with your API endpoint
            const response = await fetch('your api here', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(userData)
            });
            
            if (response.ok) {
                // Registration successful - redirect to login
                window.location.href = 'login.html';
            } else {
                // Handle error response
                const errorData = await response.json();
                console.error('Registration failed:', errorData);
                alert(`Ошибка регистрации: ${errorData.message || 'Что-то пошло не так'}`);
            }
        } catch (error) {
            console.error('Registration error:', error);
            alert('Ошибка при отправке запроса. Пожалуйста, попробуйте снова.');
        } finally {
            submitButton.disabled = false;
            submitButton.textContent = 'Зарегистрироваться';
        }
    });
    
    // Initialize validation for username field
    usernameInput.addEventListener('input', updateSubmitButton);
});
