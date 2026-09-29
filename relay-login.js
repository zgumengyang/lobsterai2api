/**
 * 反代后台登录 → 自动抓「登录态」到剪贴板（跟龙虾那个 login-local.bat 一个体验）
 *
 * 用法:
 *   node relay-login.js                       # 默认 https://tierflow.cn/register?referral_code=JHOl3i4gvtI0
 *   node relay-login.js https://yuzu.p8.ink   # 指定站
 *   node relay-login.js <站点> --port 9223
 *
 * 原理：用临时 profile 起一个带 remote-debugging 的 Edge 打开该站，
 * 你在里面正常登录；脚本每 1.5 秒通过 CDP 读一次 localStorage + Cookie（含 HttpOnly），
 * 一检测到登录成功就打包成 JSON 复制到剪贴板并打印，然后关掉浏览器。
 * 拿到的这串直接粘到面板「上游反代 → 后台登录」的登录态框里即可。
 */
const { spawn } = require('child_process');
const { execFileSync } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');

const args = process.argv.slice(2);
let site = '';
let port = 9223;
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--port') { port = parseInt(args[++i], 10) || 9223; continue; }
  if (!site) site = args[i];
}
// 默认落地页 = 注册页（带邀请码）；已有账号的话页面上点「登录」即可，一样能抓登录态
const DEFAULT_SITE = 'https://tierflow.cn/register?referral_code=JHOl3i4gvtI0';
if (!site) site = DEFAULT_SITE;
if (!/^https?:\/\//i.test(site) && !/^file:\/\//i.test(site)) site = 'https://' + site;

const host = (() => { try { return new URL(site).host; } catch (e) { return ''; } })();
const siteURLs = (() => {
  try {
    const u = new URL(site);
    if (u.protocol === 'file:') return [];
    const origin = u.origin;
    const names = origin.replace(/^https?:\/\//, '').split('.');
    const cookies = [];
    for (let i = 0; i < names.length - 1; i++) cookies.push(origin.split('//')[1].split('.').slice(i).join('.'));
    return [origin + '/', u.href].concat(cookies.map(d => 'https://' + d + '/'));
  } catch (e) { return []; }
})();

const EDGE = [
  'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
  'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe',
  'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
  'C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe',
].find(p => { try { return fs.existsSync(p); } catch (e) { return false; } });

const sleep = ms => new Promise(r => setTimeout(r, ms));
const log = (...a) => console.log(...a);

async function fetchJSON(url) {
  const r = await fetch(url);
  return await r.json();
}

// 极简 CDP 客户端（Node 22+ 自带全局 WebSocket，不需要装任何依赖）
class CDP {
  constructor(ws) {
    this.ws = ws;
    this.id = 0;
    this.pending = new Map();
    ws.addEventListener('message', ev => {
      let m;
      try { m = JSON.parse(ev.data); } catch (e) { return; }
      if (m.id && this.pending.has(m.id)) {
        const { resolve, reject } = this.pending.get(m.id);
        this.pending.delete(m.id);
        if (m.error) reject(new Error(m.error.message)); else resolve(m.result);
      }
    });
  }
  send(method, params) {
    const id = ++this.id;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params: params || {} }));
      setTimeout(() => {
        if (this.pending.has(id)) { this.pending.delete(id); reject(new Error('CDP 超时: ' + method)); }
      }, 15000);
    });
  }
}

function copyToClipboard(text) {
  try {
    execFileSync('clip.exe', { input: Buffer.from(text, 'utf8'), windowsHide: true });
    return true;
  } catch (e) {
    return false;
  }
}

function pickTarget(targets) {
  const pages = (targets || []).filter(t => t.type === 'page' && t.webSocketDebuggerUrl);
  if (!pages.length) return null;
  if (!host) return pages[0];
  const hit = pages.filter(t => (t.url || '').indexOf(host) >= 0);
  return hit.length ? hit[hit.length - 1] : pages[pages.length - 1];
}

async function grab(cdp) {
  const out = { user: '', uid: '', cookie: '', url: '' };
  try {
    const r = await cdp.send('Runtime.evaluate', {
      expression: "(function(){try{var u=localStorage.getItem('user')||'';var i=localStorage.getItem('uid')||'';var t=localStorage.getItem('token')||'';var c='';try{c=document.cookie||'';}catch(e){}" +
        "return JSON.stringify({user:u,uid:i,token:t,cookie:c,url:location.href});}catch(e){return JSON.stringify({err:String(e)})}})()",
      returnByValue: true,
    });
    const v = r && r.result && r.result.value;
    if (v) Object.assign(out, JSON.parse(v));
  } catch (e) { /* 页面可能还没加载好 */ }
  // HttpOnly 的 Cookie 拿不到 document.cookie，走 CDP 的 Network.getCookies
  if (siteURLs.length) {
    try {
      const ck = await cdp.send('Network.getCookies', { urls: siteURLs });
      const list = (ck && ck.cookies) || [];
      if (list.length) {
        const s = list.map(c => c.name + '=' + c.value).join('; ');
        if (s.length > (out.cookie || '').length) out.cookie = s;
      }
    } catch (e) {}
  }
  return out;
}

(async () => {
  log('============================================================');
  log('  反代后台登录 → 自动复制「登录态」');
  log('  站点: ' + site);
  log('============================================================');
  if (!EDGE) {
    log('[错误] 找不到 Edge/Chrome，请手动装一个再跑。');
    process.exit(1);
  }

  const profile = path.join(os.tmpdir(), 'relay-login-profile-' + Date.now());
  const isEdge = /msedge\.exe$/i.test(EDGE);
  log('[1/4] 启动浏览器（无痕模式 + 独立临时 profile，跟你平时的浏览器互不影响）...');
  const child = spawn(EDGE, [
    '--remote-debugging-port=' + port,
    '--user-data-dir=' + profile,
    // 无痕：Edge 认 --inprivate，Chrome 认 --incognito；两个都传，认不出的会被忽略
    isEdge ? '--inprivate' : '--incognito',
    '--incognito',
    '--no-first-run', '--no-default-browser-check',
    '--disable-features=msEdgeFirstRunExperience,msEdgeSidebarV2',
    site,
  ], { stdio: 'ignore', detached: false });
  // Ctrl+C / 关窗口时别留下浏览器和临时 profile
  const cleanup = () => {
    try { child.kill(); } catch (e) {}
    try { fs.rmSync(profile, { recursive: true, force: true }); } catch (e) {}
  };
  process.on('SIGINT', () => { cleanup(); process.exit(130); });

  // 等 CDP 端口起来
  let ver = null;
  for (let i = 0; i < 40; i++) {
    await sleep(500);
    try { ver = await fetchJSON('http://127.0.0.1:' + port + '/json/version'); break; } catch (e) {}
  }
  if (!ver) {
    log('[错误] 浏览器没起来（拿不到 CDP 端口 ' + port + '）。换个端口试试：--port 9224');
    try { child.kill(); } catch (e) {}
    process.exit(1);
  }
  log('      浏览器已就绪: ' + (ver.Browser || 'Edge'));
  log('');
  log('[2/4] 请在刚打开的窗口里登录 -> ' + site);
  log('      登录成功后本脚本会自动抓取并复制到剪贴板（最多等 10 分钟）');
  log('');

  let cdp = null, curUrl = '';
  let baseCookieNames = '';
  const deadline = Date.now() + 10 * 60 * 1000;
  let lastHint = 0;

  while (Date.now() < deadline) {
    await sleep(1500);
    let t = null;
    try { t = pickTarget(await fetchJSON('http://127.0.0.1:' + port + '/json')); } catch (e) {}
    if (!t) continue;
    if (!cdp || t.webSocketDebuggerUrl !== curUrl) {
      try {
        const ws = new WebSocket(t.webSocketDebuggerUrl);
        await new Promise((res, rej) => {
          ws.addEventListener('open', res);
          ws.addEventListener('error', rej);
          setTimeout(rej, 8000);
        });
        cdp = new CDP(ws);
        curUrl = t.webSocketDebuggerUrl;
        await cdp.send('Runtime.enable').catch(() => {});
        await cdp.send('Network.enable').catch(() => {});
      } catch (e) { cdp = null; continue; }
    }
    let g;
    try { g = await grab(cdp); } catch (e) { continue; }

    const names = (g.cookie || '').split(';').map(s => s.split('=')[0].trim()).filter(Boolean).sort().join(',');
    if (baseCookieNames === '') { baseCookieNames = names; }
    const newCookie = names !== baseCookieNames;
    const longCookie = (g.cookie || '').split(';').some(s => s.split('=')[1] && s.split('=')[1].trim().length >= 16);
    // 判定"登录成功"：localStorage 里出现了 user（一段 JSON）/ uid，或者冒出了新的长 Cookie
    const userLooksReal = !!(g.user && g.user.length > 10 && g.user.indexOf('{') >= 0);
    const loggedIn = userLooksReal || !!g.uid || (newCookie && longCookie);
    if (loggedIn) {
      const blob = JSON.stringify({ user: g.user || '', uid: g.uid || '', cookie: g.cookie || '' });
      const ok = copyToClipboard(blob);
      log('[3/4] 登录成功，已抓取登录态' + (ok ? '，并复制到剪贴板 ✅' : '（⚠ 复制到剪贴板失败，请从下面手动复制）'));
      log('');
      log('------------------------------------------------------------');
      log(blob);
      log('------------------------------------------------------------');
      log('');
      log('[4/4] 下一步：打开面板 → 上游反代 → 该反代卡片点「后台登录」');
      log('      → 在「登录态」框里 Ctrl+V → 点「保存并验证」');
      log('');
      try { child.kill(); } catch (e) {}
      try { fs.rmSync(profile, { recursive: true, force: true }); } catch (e) {}
      process.exit(0);
    }
    if (Date.now() - lastHint > 20000) {
      lastHint = Date.now();
      log('      ...还在等登录（当前页面: ' + (g.url || t.url || '').slice(0, 80) + '）');
    }
  }
  log('[超时] 10 分钟没检测到登录成功。');
  cleanup();
  process.exit(1);
})().catch(e => {
  console.log('[错误] ' + (e && e.message ? e.message : e));
  process.exit(1);
});
