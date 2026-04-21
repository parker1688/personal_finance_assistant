/**
 * 主JavaScript文件 - main.js
 * 个人AI理财助手前端交互
 */

// 全局变量
let refreshInterval = null;
let currentPage = 1;

// ==================== 工具函数 ====================

/**
 * 格式化日期
 */
function formatDate(date, format = 'YYYY-MM-DD') {
    const d = new Date(date);
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    const hours = String(d.getHours()).padStart(2, '0');
    const minutes = String(d.getMinutes()).padStart(2, '0');
    const seconds = String(d.getSeconds()).padStart(2, '0');
    
    return format
        .replace('YYYY', year)
        .replace('MM', month)
        .replace('DD', day)
        .replace('HH', hours)
        .replace('mm', minutes)
        .replace('ss', seconds);
}

/**
 * 格式化数字
 */
function formatNumber(num, decimals = 2) {
    if (num === undefined || num === null) return '--';
    return Number(num).toLocaleString('zh-CN', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    });
}

/**
 * 格式化货币
 */
function formatCurrency(value, currency = 'CNY') {
    if (value === undefined || value === null) return '--';
    
    const symbols = {
        'CNY': '¥',
        'USD': '$',
        'HKD': 'HK$'
    };
    
    const symbol = symbols[currency] || '¥';
    return `${symbol}${formatNumber(value)}`;
}

/**
 * 格式化百分比
 */
function formatPercent(value, decimals = 1) {
    if (value === undefined || value === null) return '--';
    const sign = value > 0 ? '+' : '';
    return `${sign}${value.toFixed(decimals)}%`;
}

/**
 * 显示提示消息
 */
function showToast(message, type = 'info') {
    const toastHtml = `
        <div class="position-fixed bottom-0 end-0 p-3" style="z-index: 9999;">
            <div class="toast align-items-center text-white bg-${type === 'success' ? 'success' : (type === 'error' ? 'danger' : 'primary')} border-0" role="alert">
                <div class="d-flex">
                    <div class="toast-body">${message}</div>
                    <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
                </div>
            </div>
        </div>
    `;
    
    $(toastHtml).appendTo('body');
    const toast = new bootstrap.Toast($('.toast').last());
    toast.show();
    
    setTimeout(() => {
        $('.toast').last().remove();
    }, 3000);
}

/**
 * 显示确认对话框
 */
function showConfirm(message, onConfirm, onCancel) {
    if (confirm(message)) {
        if (onConfirm) onConfirm();
    } else {
        if (onCancel) onCancel();
    }
}

/**
 * 复制到剪贴板
 */
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(() => {
        showToast('已复制到剪贴板', 'success');
    }).catch(() => {
        showToast('复制失败', 'error');
    });
}

/**
 * 下载文件
 */
function downloadFile(content, filename, type = 'text/csv') {
    const blob = new Blob([content], { type });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
}

/**
 * 防抖函数
 */
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

/**
 * 节流函数
 */
function throttle(func, limit) {
    let inThrottle;
    return function(...args) {
        if (!inThrottle) {
            func.apply(this, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
        }
    };
}

// ==================== AJAX 拦截器 ====================

// 请求拦截器
$.ajaxSetup({
    beforeSend: function(xhr, settings) {
        // 添加加载状态
        if (settings.type !== 'GET' || settings.url.indexOf('/api/') !== -1) {
            $('body').addClass('ajax-loading');
        }
    },
    complete: function() {
        $('body').removeClass('ajax-loading');
    }
});

// 全局错误处理
$(document).ajaxError(function(event, xhr, settings, error) {
    console.error('AJAX Error:', error);
    
    if (xhr.status === 401) {
        showToast('请重新登录', 'error');
        setTimeout(() => {
            window.location.href = '/login';
        }, 2000);
    } else if (xhr.status === 403) {
        showToast('权限不足', 'error');
    } else if (xhr.status === 404) {
        showToast('请求的资源不存在', 'error');
    } else if (xhr.status === 500) {
        showToast('服务器错误，请稍后重试', 'error');
    } else if (error !== 'abort') {
        showToast('网络错误，请检查连接', 'error');
    }
});

// ==================== 页面初始化 ====================

$(document).ready(function() {
    // 更新时间显示
    function updateDateTime() {
        const now = new Date();
        const year = now.getFullYear();
        const month = String(now.getMonth() + 1).padStart(2, '0');
        const day = String(now.getDate()).padStart(2, '0');
        const hours = String(now.getHours()).padStart(2, '0');
        const minutes = String(now.getMinutes()).padStart(2, '0');
        const seconds = String(now.getSeconds()).padStart(2, '0');
        
        $('.datetime').html(`${year}-${month}-${day} ${hours}:${minutes}:${seconds}`);
    }
    
    updateDateTime();
    setInterval(updateDateTime, 1000);
    
    // 移动端菜单切换
    $('#menuToggle').on('click', function() {
        $('#sidebar').toggleClass('show');
    });
    
    // 点击页面其他地方关闭侧边栏（移动端）
    $(document).on('click', function(e) {
        if ($(window).width() <= 768) {
            if (!$(e.target).closest('#sidebar').length && !$(e.target).closest('#menuToggle').length) {
                $('#sidebar').removeClass('show');
            }
        }
    });
    
    // 初始化所有工具提示
    $('[data-bs-toggle="tooltip"]').tooltip();
    
    // 初始化所有弹出框
    $('[data-bs-toggle="popover"]').popover();
    
    console.log('页面初始化完成');
});

// ==================== 图表辅助函数 ====================

/**
 * 创建折线图
 */
function createLineChart(elementId, data, options = {}) {
    const chart = echarts.init(document.getElementById(elementId));
    
    const defaultOptions = {
        tooltip: { trigger: 'axis' },
        grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
        xAxis: { type: 'category', data: data.dates || [] },
        yAxis: { type: 'value', name: options.yAxisName || '值' },
        series: [{
            name: options.seriesName || '数据',
            type: 'line',
            data: data.values || [],
            smooth: true,
            lineStyle: { color: '#00d4ff', width: 3 },
            areaStyle: { color: 'rgba(0, 212, 255, 0.1)' }
        }]
    };
    
    chart.setOption({ ...defaultOptions, ...options });
    window.addEventListener('resize', () => chart.resize());
    
    return chart;
}

/**
 * 创建柱状图
 */
function createBarChart(elementId, data, options = {}) {
    const chart = echarts.init(document.getElementById(elementId));
    
    const defaultOptions = {
        tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
        grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
        xAxis: { type: 'category', data: data.categories || [] },
        yAxis: { type: 'value', name: options.yAxisName || '值' },
        series: [{
            name: options.seriesName || '数据',
            type: 'bar',
            data: data.values || [],
            itemStyle: {
                color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                    { offset: 0, color: '#00d4ff' },
                    { offset: 1, color: '#0099cc' }
                ]),
                borderRadius: [4, 4, 0, 0]
            }
        }]
    };
    
    chart.setOption({ ...defaultOptions, ...options });
    window.addEventListener('resize', () => chart.resize());
    
    return chart;
}

/**
 * 创建饼图
 */
function createPieChart(elementId, data, options = {}) {
    const chart = echarts.init(document.getElementById(elementId));
    
    const defaultOptions = {
        tooltip: { trigger: 'item' },
        legend: { orient: 'vertical', left: 'left' },
        series: [{
            name: options.seriesName || '分布',
            type: 'pie',
            radius: '50%',
            data: data,
            emphasis: { itemStyle: { shadowBlur: 10, shadowOffsetX: 0 } }
        }]
    };
    
    chart.setOption({ ...defaultOptions, ...options });
    window.addEventListener('resize', () => chart.resize());
    
    return chart;
}

/**
 * 创建雷达图
 */
function createRadarChart(elementId, indicators, values, options = {}) {
    const chart = echarts.init(document.getElementById(elementId));
    
    const defaultOptions = {
        tooltip: { trigger: 'item' },
        radar: {
            indicator: indicators,
            shape: 'circle',
            center: ['50%', '50%'],
            radius: '65%'
        },
        series: [{
            type: 'radar',
            data: [{ value: values, name: options.seriesName || '评分' }],
            areaStyle: { color: 'rgba(0, 212, 255, 0.3)' },
            lineStyle: { color: '#00d4ff', width: 2 },
            itemStyle: { color: '#0099cc' }
        }]
    };
    
    chart.setOption({ ...defaultOptions, ...options });
    window.addEventListener('resize', () => chart.resize());
    
    return chart;
}

/**
 * 创建仪表盘图
 */
function createGaugeChart(elementId, value, options = {}) {
    const chart = echarts.init(document.getElementById(elementId));
    
    const defaultOptions = {
        series: [{
            type: 'gauge',
            center: ['50%', '50%'],
            radius: '70%',
            min: options.min || 0,
            max: options.max || 100,
            splitNumber: options.splitNumber || 5,
            axisLine: {
                lineStyle: {
                    width: 15,
                    color: [
                        [0.3, '#6bcb77'],
                        [0.7, '#ffd93d'],
                        [1, '#ff6b6b']
                    ]
                }
            },
            pointer: { show: true },
            detail: { formatter: '{value}' },
            title: { show: false },
            data: [{ value: value, name: options.title || '' }]
        }]
    };
    
    chart.setOption({ ...defaultOptions, ...options });
    window.addEventListener('resize', () => chart.resize());
    
    return chart;
}

// 导出工具函数（全局）
window.formatDate = formatDate;
window.formatNumber = formatNumber;
window.formatCurrency = formatCurrency;
window.formatPercent = formatPercent;
window.showToast = showToast;
window.showConfirm = showConfirm;
window.copyToClipboard = copyToClipboard;
window.downloadFile = downloadFile;
window.debounce = debounce;
window.throttle = throttle;
window.createLineChart = createLineChart;
window.createBarChart = createBarChart;
window.createPieChart = createPieChart;
window.createRadarChart = createRadarChart;
window.createGaugeChart = createGaugeChart;