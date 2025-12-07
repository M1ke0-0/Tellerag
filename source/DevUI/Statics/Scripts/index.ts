// Interface definitions
interface SystemMetrics {
    cpuUsage: number;
    memoryUsage: number;
    currentMalloc: number;
    peakMalloc: number;
    loadAverage: number;
}

interface Process {
    pid: number;
    name: string;
    cpuUsage: number;
    memoryUsage: number;
    threads: number;
    io: number;
    openFiles: number;
}

// Theme management
const THEME_KEY = 'telerag_theme_preference';
enum Theme {
    Light = 'light',
    Dark = 'dark'
}

function setTheme(theme: Theme): void {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem(THEME_KEY, theme);
}

function toggleTheme(): void {
    const currentTheme = document.documentElement.getAttribute('data-theme') as Theme;
    const newTheme = currentTheme === Theme.Light ? Theme.Dark : Theme.Light;
    setTheme(newTheme);
}

function loadSavedTheme(): void {
    const savedTheme = localStorage.getItem(THEME_KEY) as Theme;
    if (savedTheme) {
        setTheme(savedTheme);
    } else {
        // Check if user prefers dark mode at system level
        if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            setTheme(Theme.Dark);
        }
    }
}

// Mock data for demonstration (replace with actual API calls)
function getMockSystemMetrics(): SystemMetrics {
    return {
        cpuUsage: Math.random() * 100,
        memoryUsage: Math.random() * 100,
        currentMalloc: Math.round(Math.random() * 8192),
        peakMalloc: Math.round(Math.random() * 10000),
        loadAverage: Math.random() * 10
    };
}

// Fetch process list from API
async function fetchProcesses(): Promise<Process[]> {
    const response = await fetch('your_api_here/process_stats');
    if (!response.ok) {
        throw new Error(`Failed to fetch process stats: ${response.statusText}`);
    }
    const processes = await response.json();
    return processes.map((pid: number) => ({
        pid,
        name: '', // No name available
        cpuUsage: Math.random() * 100, // Placeholder, replace if API provides this
        memoryUsage: Math.random() * 100, // Placeholder, replace if API provides this
        threads: Math.floor(1 + Math.random() * 20), // Placeholder
        io: Math.floor(Math.random() * 1000), // Placeholder
        openFiles: Math.floor(Math.random() * 100) // Placeholder
    }));
}

// DOM Elements
const cpuUsageElement = document.getElementById('cpu-usage') as HTMLElement;
const cpuProgressElement = document.getElementById('cpu-progress') as HTMLElement;
const memoryUsageElement = document.getElementById('memory-usage') as HTMLElement;
const memoryProgressElement = document.getElementById('memory-progress') as HTMLElement;
const currentMallocElement = document.getElementById('current-malloc') as HTMLElement;
const peakMallocElement = document.getElementById('peak-malloc') as HTMLElement;
const loadAverageElement = document.getElementById('load-average') as HTMLElement;
const loadCircleFillElement = document.getElementById('load-circle-fill') as HTMLElement;
const processListElement = document.getElementById('process-list') as HTMLElement;
const processSearchElement = document.getElementById('process-search') as HTMLInputElement;
const processSortElement = document.getElementById('process-sort') as HTMLSelectElement;
const requestMethodElement = document.getElementById('request-method') as HTMLSelectElement;
const requestUrlElement = document.getElementById('request-url') as HTMLInputElement;
const requestBodyElement = document.getElementById('request-body') as HTMLTextAreaElement;
const responseAreaElement = document.getElementById('response-area') as HTMLElement;
const sendRequestElement = document.getElementById('send-request') as HTMLButtonElement;
const themeToggleElement = document.getElementById('theme-toggle') as HTMLButtonElement;

// Update system metrics display
function updateSystemMetrics(metrics: SystemMetrics): void {
    // Update CPU usage
    cpuUsageElement.textContent = `${metrics.cpuUsage.toFixed(1)}%`;
    cpuProgressElement.style.width = `${metrics.cpuUsage}%`;
    
    // Update memory usage
    memoryUsageElement.textContent = `${metrics.memoryUsage.toFixed(1)}%`;
    memoryProgressElement.style.width = `${metrics.memoryUsage}%`;
    
    // Update malloc metrics
    currentMallocElement.textContent = `${metrics.currentMalloc.toFixed(0)} MB`;
    peakMallocElement.textContent = `${metrics.peakMalloc.toFixed(0)} MB`;
    
    // Update load average with color indication
    loadAverageElement.textContent = metrics.loadAverage.toFixed(2);
    
    // Change color for high load average
    if (metrics.loadAverage > 5) {
        loadAverageElement.classList.add('load-high');
    } else {
        loadAverageElement.classList.remove('load-high');
    }
    
    // Update load circle fill
    updateLoadCircle(metrics.loadAverage);
}

// Update the load circle fill based on load average value
function updateLoadCircle(loadValue: number): void {
    // Calculate percentage (assuming 10 is max)
    const percentage = Math.min(loadValue / 10, 1) * 100;
    
    // For a full circle, we need to handle the clip-path differently based on percentage
    if (percentage <= 50) {
        loadCircleFillElement.style.clipPath = `polygon(50% 0%, 50% 50%, ${50 + 50 * Math.sin(percentage/50 * Math.PI)}% ${50 - 50 * Math.cos(percentage/50 * Math.PI)}%, 50% 0%)`;
    } else {
        loadCircleFillElement.style.clipPath = `polygon(50% 0%, 50% 50%, 100% 50%, 100% ${50 - 50 * Math.sin((percentage-50)/50 * Math.PI)}%, ${50 + 50 * Math.cos((percentage-50)/50 * Math.PI)}% 0%)`;
    }
    
    // Change color based on load
    if (percentage > 50) {
        loadCircleFillElement.style.backgroundColor = '#e74c3c';
    } else {
        loadCircleFillElement.style.backgroundColor = '#4a90e2';
    }
}

// Render process list
function renderProcesses(processes: Process[]): void {
    processListElement.innerHTML = '';
    
    processes.forEach(process => {
        const row = document.createElement('tr');
        
        row.innerHTML = `
            <td>${process.pid}</td>
            <td>${process.name}</td>
            <td>${process.cpuUsage.toFixed(1)}%</td>
            <td>${process.memoryUsage.toFixed(1)}%</td>
            <td>${process.threads}</td>
            <td>${process.io}</td>
            <td>${process.openFiles}</td>
        `;
        
        processListElement.appendChild(row);
    });
}

// Filter and sort processes
function filterAndSortProcesses(processes: Process[]): Process[] {
    const searchTerm = processSearchElement.value.toLowerCase();
    const sortBy = processSortElement.value;
    
    // Filter by search term
    let filtered = processes.filter(process => 
        process.name.toLowerCase().includes(searchTerm) || 
        process.pid.toString().includes(searchTerm)
    );
    
    // Sort by selected criteria
    filtered.sort((a, b) => {
        switch(sortBy) {
            case 'cpu':
                return b.cpuUsage - a.cpuUsage;
            case 'memory':
                return b.memoryUsage - a.memoryUsage;
            case 'pid':
                return a.pid - b.pid;
            default:
                return 0;
        }
    });
    
    return filtered;
}

// Handle API request
function handleApiRequest(): void {
    const method = requestMethodElement.value;
    const url = requestUrlElement.value;
    let body: string | null = null;
    
    if (method !== 'GET' && requestBodyElement.value.trim() !== '') {
        body = requestBodyElement.value;
    }
    
    // Display request processing
    responseAreaElement.innerText = 'Processing request...';
    
    // Mock API response
    setTimeout(() => {
        try {
            // Simulate a response (replace with actual fetch call)
            const response = {
                status: 200,
                statusText: 'OK',
                data: {
                    message: 'Request processed successfully',
                    method,
                    url,
                    receivedBody: body ? JSON.parse(body) : null,
                    timestamp: new Date().toISOString()
                }
            };
            
            responseAreaElement.innerText = JSON.stringify(response, null, 2);
        } catch (error) {
            responseAreaElement.innerText = `Error: ${error instanceof Error ? error.message : 'Unknown error'}`;
        }
    }, 500);
}

// Initialize event listeners
function initEventListeners(): void {
    processSearchElement.addEventListener('input', () => {
        renderProcesses(filterAndSortProcesses(getMockProcesses(20)));
    });
    
    processSortElement.addEventListener('change', () => {
        renderProcesses(filterAndSortProcesses(getMockProcesses(20)));
    });
    
    sendRequestElement.addEventListener('click', handleApiRequest);
    
    // Theme toggle event listener
    themeToggleElement.addEventListener('click', toggleTheme);
}

// Initialize the dashboard
function initDashboard(): void {
    // Load saved theme preference
    loadSavedTheme();

    // Fetch and update system metrics
    const updateMetrics = async () => {
        try {
            const metrics = await fetchSystemMetrics();
            updateSystemMetrics(metrics);
        } catch (error) {
            console.error(error);
        }
    };

    // Fetch and render processes
    const updateProcesses = async () => {
        try {
            const processes = await fetchProcesses();
            renderProcesses(filterAndSortProcesses(processes));
        } catch (error) {
            console.error(error);
        }
    };

    // Set initial data
    updateMetrics();
    updateProcesses();

    // Set up event listeners
    initEventListeners();

    // Set up periodic updates
    setInterval(updateMetrics, 2000);
    setInterval(updateProcesses, 5000);
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', initDashboard);

