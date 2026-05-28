
window.onerror = function(msg, url, line, col, error) {
    const div = document.createElement('div');
    div.style.position = 'fixed';
    div.style.top = '10px';
    div.style.left = '10px';
    div.style.zIndex = '99999';
    div.style.background = 'white';
    div.style.color = 'red';
    div.style.padding = '10px';
    div.style.border = '2px solid red';
    div.innerHTML = 'JS ERROR: ' + msg + ' at line ' + line + ':' + col + '<br><pre>' + (error ? error.stack : '') + '</pre>';
    document.body.appendChild(div);
};
window.addEventListener('unhandledrejection', function(event) {
    window.onerror(event.reason, null, 0, 0, event.reason);
});

