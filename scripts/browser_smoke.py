"""Real browser scenario against loopback DEMO; never contacts ninef.

--offline-bridge is an explicitly limited fallback for browser network restrictions:
actual HTML/JS and FastAPI execute, but fetch uses an in-process HTTP bridge and
EventSource is stubbed. It proves polling UI, not browser SSE/cookie transport.
"""
import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from urllib.request import urlopen

from fastapi.testclient import TestClient
from playwright.sync_api import sync_playwright

from vibe_trading.api import WebSettings, create_app
from vibe_trading.demo import DemoProvider
from vibe_trading.service import Workbench
from vibe_trading.store import Store

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(os.environ.get('VIBE_SCREENSHOT_DIR', str(ROOT / 'test-results')))


@contextmanager
def network_server(temp):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    env = os.environ | {'PYTHONPATH': str(ROOT/'src'), 'VIBE_UI_PASSWORD': 'demo'}
    with (OUT/'server.log').open('w') as log:
        process = subprocess.Popen([sys.executable, '-m', 'vibe_trading.cli', 'serve',
            '--mode', 'demo', '--db', str(Path(temp)/'demo.db'), '--port', str(port)],
            cwd=ROOT, env=env, stdout=log, stderr=log)
        try:
            url = f'http://127.0.0.1:{port}'
            for _ in range(100):
                try:
                    if urlopen(url, timeout=1).status == 200: break
                except Exception: time.sleep(0.1)
            else: raise RuntimeError('DEMO server did not start')
            yield url
        finally:
            process.terminate()
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired: process.kill(); process.wait()


def exercise(make_page, errors, offline):
    page = make_page()
    page.locator('#password').fill('demo')
    page.locator('#login-form button').click()
    page.locator('#assets').filter(has_text='100,000').wait_for()
    assert page.locator('#rank-body tr').count() == 30
    assert '117' in page.locator('#rank').inner_text()
    assert page.locator('#news-list article').count() == 3
    page.locator('#start').click()
    page.wait_for_function("document.querySelector('#run-state').textContent==='COMPLETED'", timeout=30000)
    page.locator('#orders-body tr').nth(3).wait_for()
    assert page.locator('#orders-body tr').count() == 4
    assert page.locator('#blockers').inner_text() == ''
    page.screenshot(path=str(OUT/'workbench-desktop.png'), full_page=True)
    old_hash = page.locator('#plan-hash').inner_text()
    page.locator('#refresh-plan').click()
    page.wait_for_function("old => document.querySelector('#plan-hash').textContent !== old", arg=old_hash)
    plan_hash = page.locator('#plan-hash').inner_text()
    if offline:
        page.close()
        page = make_page()
    else:
        page.reload()
    page.locator('#plan-hash').filter(has_text=plan_hash).wait_for()
    page.locator('#acknowledge').check()
    page.locator('#approve').click()
    page.wait_for_function("document.querySelector('#batch-state').textContent==='COMPLETED'", timeout=30000)
    assert page.locator('#execution-body').inner_text().count('FILLED') == 4
    page.evaluate('window.scrollTo(0,0)')
    page.screenshot(path=str(OUT/'workbench-completed.png'), full_page=True)
    page.locator('#evidence').click()
    page.wait_for_function("document.querySelector('#evidence-text').textContent.includes('news_hash')")
    page.locator('#close-evidence').click()
    page.set_viewport_size({'width': 390, 'height': 844})
    page.evaluate('window.scrollTo(0,0)')
    page.screenshot(path=str(OUT/'workbench-mobile.png'), full_page=True)
    assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
    assert not errors, errors


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--offline-bridge', action='store_true')
    args = parser.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    errors, streams = [], []
    with tempfile.TemporaryDirectory() as temp, sync_playwright() as playwright:
        executable = os.environ.get('PLAYWRIGHT_CHROMIUM_EXECUTABLE') or shutil.which('chromium')
        options = {'headless': True, 'args': ['--no-sandbox']}
        if executable: options['executable_path'] = executable
        browser = playwright.chromium.launch(**options)
        def page_base():
            page = browser.new_page(viewport={'width': 1440, 'height': 1100})
            page.on('pageerror', lambda e: errors.append(str(e)))
            page.on('response', lambda r: streams.append(r.url) if '/stream' in r.url else None)
            return page
        try:
            if args.offline_bridge:
                store = Store(Path(temp)/'demo.db')
                bench = Workbench(store, DemoProvider(store, delay=0.015), enabled=True, poll_seconds=0.1)
                with TestClient(create_app(bench, WebSettings('demo')), base_url='http://localhost') as client:
                    def bridge(url, opts):
                        reply = client.request(opts.get('method', 'GET'), url,
                            headers=opts.get('headers', {}) | {'origin': 'http://localhost'},
                            content=opts.get('body'))
                        return {'status': reply.status_code, 'body': reply.text}
                    def make_page():
                        page = page_base()
                        page.expose_function('httpBridge', bridge)
                        html = (ROOT/'src/vibe_trading/web/index.html').read_text()
                        html = html.replace('<link rel="stylesheet" href="/static/style.css">', '')
                        html = html.replace('<script src="/static/app.js" defer></script>', '')
                        page.set_content(html)
                        page.add_style_tag(path=str(ROOT/'src/vibe_trading/web/style.css'))
                        page.evaluate('''() => {
                          window.fetch=async(url,options={})=>{const r=await window.httpBridge(url,options);return new Response(r.body,{status:r.status,headers:{'Content-Type':'application/json'}});};
                          window.EventSource=class{addEventListener(){} close(){}};
                        }''')
                        page.add_script_tag(path=str(ROOT/'src/vibe_trading/web/app.js'))
                        return page
                    exercise(make_page, errors, True)
            else:
                with network_server(temp) as url:
                    def make_page():
                        page = page_base(); page.goto(url); return page
                    exercise(make_page, errors, False)
                    assert streams, 'No real SSE connection observed'
        finally:
            browser.close()
    report = {'result': 'PASS', 'mode': 'OFFLINE_HTTP_BRIDGE' if args.offline_bridge else 'NETWORK_BROWSER',
        'page_errors': errors, 'sse_connections': len(streams), 'external_broker_writes': 0,
        'checks': ['login', 'top30', 'own rank', 'news', 'progress', 'plan', 'rehydration',
                   'approval', 'fills', 'evidence', '390px layout'],
        'not_verified': ['current ninef', 'current AI'] +
            (['browser network SSE', 'browser cookie transport'] if args.offline_bridge else [])}
    (OUT/'browser-report.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__': main()
