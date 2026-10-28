// mongle p (no installs needed, just Node.js 18+)
const NAME = "mongle p";
// SPEED: Node resolves names on only 4 background threads by default. A page that tries dozens of
// ad/tracker hosts (many of which fail slowly) can clog all 4 and stall everything else.
if (!process.env.UV_THREADPOOL_SIZE) process.env.UV_THREADPOOL_SIZE = "64";
const http = require("http");
const zlib = require("zlib");
const { promisify } = require("util");
const { pipeline } = require("stream");
const brotliAsync = promisify(zlib.brotliCompress);
const gzipAsync = promisify(zlib.gzip);
const { Readable } = require("stream");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const dns = require("dns");
const net = require("net");
const tls = require("tls");
const https = require("https");

// =====================================================================================
// ERROR LOG: so a crash shows up on a page you can open in a browser (http://host/__errors),
// even on a host like Render where you can't see the server's own console.
// =====================================================================================
const ERR_LOG = [];
function noteError(kind, err) {
  const e = {
    t: Date.now(),
    kind,
    message: String((err && err.message) || err),
    stack: String((err && err.stack) || ""),
  };
  ERR_LOG.push(e);
  if (ERR_LOG.length > 200) ERR_LOG.shift();
  console.error(`[${NAME}] ${kind}:`, (err && err.stack) || err);
}
// A crash outside any request (a stray throw, a rejected promise nobody caught) used to kill
// the whole server with no trace anywhere you could see on a host. Now it's logged and the
// server keeps running - open /__errors to see what happened.
process.on("uncaughtException", (err) => noteError("uncaughtException (would have crashed the server)", err));
process.on("unhandledRejection", (err) => noteError("unhandledRejection (would have crashed the server)", err));
function errorsPage(asJson) {
  if (asJson) return JSON.stringify(ERR_LOG.slice().reverse());
  const rows = ERR_LOG
    .slice()
    .reverse()
    .map(
      (e) =>
        `<tr><td>${new Date(e.t).toLocaleString()}</td><td>${esc(e.kind)}</td><td>${esc(e.message)}</td></tr><tr><td></td><td colspan="2"><pre style="white-space:pre-wrap;color:#9fe;margin:2px 0 10px">${esc(e.stack)}</pre></td></tr>`
    )
    .join("");
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><title>${NAME} - errors</title>
<meta http-equiv="refresh" content="5">
<style>body{background:#000;color:#0f4;font-family:monospace;padding:20px;max-width:1000px;margin:auto}
table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #144;padding:4px 8px;text-align:left;font-size:13px;word-break:break-word;vertical-align:top}
th{color:#9fe}a{color:#7cff9c}</style></head><body>
<h1>errors</h1>
<p><a href="/">&larr; back</a> &nbsp; <a href="/__errors">refresh</a> &nbsp; <a href="/__errors?clear=1">clear</a> &nbsp; <a href="/__errors?json=1">as json</a></p>
<p style="color:#6a8;font-size:13px">Every crash and request error this server has hit since it started, newest first. This page refreshes itself every 5 seconds. If this is empty but something still looks broken, the problem is likely in your own browser (open its devtools console) rather than on the server.</p>
<table><tr><th>when</th><th>what</th><th>message</th></tr>${rows || '<tr><td colspan="3" style="color:#6a8">no errors recorded yet</td></tr>'}</table>
</body></html>`;
}

// SPEED: remember DNS answers (60 s) and failures (30 s) so repeated lookups are instant.
{
  const origLookup = dns.lookup;
  const lc = new Map();
  dns.lookup = function (host, opts, cb) {
    if (typeof opts === "function") {
      cb = opts;
      opts = {};
    } else if (typeof opts === "number") opts = { family: opts };
    opts = opts || {};
    if (typeof host !== "string" || net.isIP(host) || typeof cb !== "function") return origLookup.call(dns, host, opts, cb);
    const key = host.toLowerCase() + "|" + (opts.family || 0) + "|" + (opts.all ? 1 : 0) + "|" + (opts.hints || 0) + "|" + (opts.order || "");
    const hit = lc.get(key);
    if (hit && hit.exp > Date.now())
      return process.nextTick(() => {
        if (hit.err) cb(hit.err);
        else if (opts.all) cb(null, hit.a.map((x) => ({ ...x })));
        else cb(null, hit.a, hit.f);
      });
    const finish = (err, a, f) => {
      if (lc.size > 3000) lc.clear();
      lc.set(key, { err, a, f, exp: Date.now() + (err ? 30000 : 60000) });
      cb(err, a, f);
    };
    origLookup.call(dns, host, opts, (err, a, f) => {
      // Your own DNS could not answer (often a network filter). Ask Cloudflare/Google over HTTPS instead.
      if (err && DOH_ON && (err.code === "ENOTFOUND" || err.code === "EAI_AGAIN") && !/\.(invalid|localhost|local|internal)$/i.test(host)) {
        return dohResolve(host, opts.family === 6 ? 28 : 1).then(
          (list) => {
            if (!list.length) return finish(err);
            logDiag(host, "info", "your DNS failed (" + err.code + "); resolved through DNS-over-HTTPS", "");
            if (opts.all) finish(null, list);
            else finish(null, list[0].address, list[0].family);
          },
          () => finish(err)
        );
      }
      finish(err, a, f);
    });
  };
}

// DNS-over-HTTPS fallback. DOH=0 turns it off; DOH_URLS="https://a/dns-query,https://b/resolve" changes the servers.
const DOH_ON = process.env.DOH !== "0";
const DOH_URLS = (process.env.DOH_URLS || "https://1.1.1.1/dns-query,https://8.8.8.8/resolve").split(",");
function dohQuery(base, host, type) {
  return new Promise((resolve, reject) => {
    const u = new URL(base);
    u.searchParams.set("name", host);
    u.searchParams.set("type", String(type));
    const rq = (u.protocol === "http:" ? http : https).get(u, { headers: { accept: "application/dns-json" }, timeout: 4000 }, (res) => {
      const chunks = [];
      res.on("data", (c) => chunks.push(c));
      res.on("end", () => {
        try {
          const j = JSON.parse(Buffer.concat(chunks).toString());
          if (j.Status === 3) return resolve([]); // the name really does not exist
          if (j.Status !== 0) return reject(new Error("doh status " + j.Status));
          resolve((j.Answer || []).filter((x) => x.type === type).map((x) => String(x.data)));
        } catch (e) {
          reject(e);
        }
      });
    });
    rq.on("timeout", () => rq.destroy(new Error("doh timeout")));
    rq.on("error", reject);
  });
}
async function dohResolve(host, type) {
  for (const base of DOH_URLS) {
    try {
      let ips = await dohQuery(base, host, type);
      if (!ips.length && type === 1) ips = await dohQuery(base, host, 28).catch(() => []);
      // never let a DNS answer point us at private/internal addresses (unless ALLOW_PRIVATE=1)
      ips = ips.filter((ip) => net.isIP(ip) && (ALLOW_PRIVATE || !isPrivateIp(ip)));
      if (ips.length) return ips.map((address) => ({ address, family: net.isIP(address) }));
      return [];
    } catch {}
  }
  return [];
}

// Prefer IPv4 when a site has both: many home networks have broken IPv6, which causes
// long "Connect Timeout" / EAI_AGAIN delays.
try {
  dns.setDefaultResultOrder("ipv4first");
} catch {}

// Newer Node versions offer a post-quantum key exchange that a few older servers
// reject with "internal error". Use the classic, universally supported curves.
try {
  tls.DEFAULT_ECDH_CURVE = "X25519:prime256v1:secp384r1";
} catch {}

const PORT = process.env.PORT || 8080;
const UA =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36";

// Simple cookie jar: hostname -> Map(name -> value)
const jar = new Map();

// Debug log: remembers recent problems per site so the test page can show them
const diag = [];
function siteKey(x) {
  try {
    const h = (x.includes("/") ? new URL(x).hostname : x).split(".");
    if (/^\d+(\.\d+){3}$/.test(h.join("."))) return h.join(".");
    return h.length > 2 ? h.slice(-2).join(".") : h.join(".");
  } catch {
    return String(x);
  }
}
function logDiag(page, type, msg, url) {
  diag.push({
    site: siteKey(page),
    type,
    msg: String(msg || "").slice(0, 300),
    url: String(url || "").slice(0, 200),
  });
  if (diag.length > 600) diag.shift();
}
function diagFor(site) {
  const counts = new Map();
  for (const d of diag) {
    if (d.site !== site) continue;
    let where = d.url;
    try {
      const u = new URL(d.url);
      where = u.origin + u.pathname;
    } catch {}
    const line = `${d.type} ${where} ${d.msg}`.trim();
    counts.set(line, (counts.get(line) || 0) + 1);
  }
  return [...counts]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 12)
    .map(([line, n]) => (n > 1 ? `${n}x ${line}` : line));
}

const prox = (u) => "/go?url=" + encodeURIComponent(u);

// ---- Safety: don't let the proxy be used to reach private/internal addresses ----
// (matters when the proxy is on a public website: otherwise anyone could poke at the
// host's internal network). Set ALLOW_PRIVATE=1 only if you really want that.
const ALLOW_PRIVATE = process.env.ALLOW_PRIVATE === "1";
function isPrivateIp(ip) {
  ip = String(ip).toLowerCase();
  const m4 = ip.match(/^(?:::ffff:)?(\d+)\.(\d+)\.(\d+)\.(\d+)$/);
  if (m4) {
    const [a, b] = [Number(m4[1]), Number(m4[2])];
    return (
      a === 0 || a === 10 || a === 127 ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      (a === 100 && b >= 64 && b <= 127) ||
      a >= 224
    );
  }
  return ip === "::" || ip === "::1" || /^f[cd]/.test(ip) || /^fe[89ab]/.test(ip);
}
// SPEED: the private-address check used to do a fresh DNS lookup for EVERY request.
// Remember the answer per host for 60 seconds.
const blockCache = new Map(); // host -> { v: boolean, exp: ms }
async function isBlockedHost(host) {
  if (ALLOW_PRIVATE) return false;
  host = String(host).replace(/^\[|\]$/g, "");
  if (/^localhost$/i.test(host)) return true;
  if (net.isIP(host)) return isPrivateIp(host);
  const hit = blockCache.get(host);
  if (hit && hit.exp > Date.now()) return hit.v;
  let v = false; // can't resolve: let the normal "could not reach" error happen
  try {
    const addrs = await dns.promises.lookup(host, { all: true });
    v = addrs.some((a) => isPrivateIp(a.address));
  } catch {}
  if (blockCache.size > 2000) blockCache.clear();
  blockCache.set(host, { v, exp: Date.now() + 60000 });
  return v;
}

// ---- SPEED: compression -------------------------------------------------------------
// fetch() unzips what the real site sent, so without this every script/page went to your
// browser at full size. We re-compress text-like files (brotli or gzip, whichever the
// browser accepts).
const COMPRESSIBLE = /^(text\/|application\/(javascript|x-javascript|ecmascript|json|xml|wasm|xhtml\+xml|manifest\+json|x-font-ttf)|image\/svg\+xml|font\/(ttf|otf)|application\/vnd\.ms-fontobject)/i;
function pickEncoding(req) {
  if (process.env.COMPRESS === "0") return null;
  const ae = String(req.headers["accept-encoding"] || "");
  if (/\bbr\b/i.test(ae)) return "br";
  if (/\bgzip\b/i.test(ae)) return "gzip";
  return null;
}
function compressBuf(buf, enc, quality) {
  return enc === "br"
    ? brotliAsync(buf, {
        params: {
          [zlib.constants.BROTLI_PARAM_QUALITY]: quality,
          [zlib.constants.BROTLI_PARAM_SIZE_HINT]: buf.length,
        },
      })
    : gzipAsync(buf, { level: 6 });
}
function makeCompressor(enc) {
  return enc === "br"
    ? zlib.createBrotliCompress({ params: { [zlib.constants.BROTLI_PARAM_QUALITY]: 4 } })
    : zlib.createGzip({ level: 5 });
}
function canCompress(status, h) {
  return status === 200 && !h["content-range"] && !h["content-encoding"] && COMPRESSIBLE.test(h["content-type"] || "");
}

// Send a whole in-memory body (compressing if the browser can take it).
async function sendBuf(req, res, status, headers, body, entry) {
  const h = { ...headers };
  if (status === 200 && h.etag && req.headers["if-none-match"] === h.etag) {
    res.writeHead(304, { etag: h.etag, "cache-control": h["cache-control"] || "no-cache" });
    return res.end();
  }
  let out = body;
  const enc = body.length >= 1024 && canCompress(status, h) ? pickEncoding(req) : null;
  if (enc) {
    if (entry) {
      entry.z = entry.z || {};
      if (!entry.z[enc]) entry.z[enc] = await compressBuf(body, enc, 6);
      out = entry.z[enc];
    } else out = await compressBuf(body, enc, 4);
    h["content-encoding"] = enc;
  }
  if (canCompress(status, headers)) h.vary = "Accept-Encoding";
  h["content-length"] = out.length;
  res.writeHead(status, h);
  res.end(req.method === "HEAD" ? undefined : out);
}

// ---- SPEED: server-side cache for static files ------------------------------------------
// Files the real site says are cacheable (max-age, immutable) are kept in memory, so a second
// visit (or a second person) doesn't wait for the real site again. CACHE_MB=0 turns it off.
const CACHE_MAX = Number(process.env.CACHE_MB === undefined ? 128 : process.env.CACHE_MB) * 1048576;
const CACHE_ITEM_MAX = 12 * 1048576;
const rcache = new Map(); // key -> entry (Map order = oldest first)
let rcacheBytes = 0;
function cacheGet(key) {
  const e = rcache.get(key);
  if (!e) return null;
  if (e.exp < Date.now()) {
    rcache.delete(key);
    rcacheBytes -= e.body.length;
    return null;
  }
  rcache.delete(key);
  rcache.set(key, e); // mark as recently used
  return e;
}
function cachePut(key, entry) {
  if (!CACHE_MAX) return;
  const old = rcache.get(key);
  if (old) {
    rcache.delete(key);
    rcacheBytes -= old.body.length;
  }
  rcache.set(key, entry);
  rcacheBytes += entry.body.length;
  for (const [k, e] of rcache) {
    if (rcacheBytes <= CACHE_MAX) break;
    rcache.delete(k);
    rcacheBytes -= e.body.length;
  }
}
// How many seconds may we keep this response? 0 = don't cache.
function cacheTtl(req, r, ct) {
  if (!CACHE_MAX || req.method !== "GET" || r.status !== 200) return 0;
  if (/text\/html|application\/xhtml/i.test(ct)) return 0;
  if (r.headers.get("set-cookie")) return 0;
  const cc = r.headers.get("cache-control") || "";
  if (/no-store|no-cache|private/i.test(cc)) return 0;
  if (/cookie|\*/i.test(r.headers.get("vary") || "")) return 0;
  let m = /s-maxage=(\d+)/i.exec(cc) || /(?:^|[,\s])max-age=(\d+)/i.exec(cc);
  if (m) return Math.min(Number(m[1]), 86400);
  return /immutable/i.test(cc) ? 86400 : 0;
}

// ---- JavaScript location fix -------------------------------------------------------------
// Pages run at localhost:8080/go?url=..., so a game that asks "am I on poxel.io?" via
// location.hostname sees "localhost" and may refuse to start. We rewrite READS of
// location.hostname / .href / .origin ... inside scripts to ask a stand-in object that answers
// with the real site's address. (Assignments like location.href = "..." are left alone.)
// REWRITE_JS=0 turns this off.
let REWRITE_JS = process.env.REWRITE_JS !== "0";
const JS_MAX = 6 * 1048576;
// Huge, finicky apps (Google/YouTube) work better with their scripts left exactly as they are.
const NO_JS_REWRITE = /(^|\.)(youtube\.com|google\.com|gstatic\.com|googleapis\.com|ytimg\.com|googlevideo\.com|googleusercontent\.com)$/i;
const LOC_RE = /(?<![\w$.])(?:window|self|document|globalThis|top|parent)\.location\.(href|origin|protocol|host|hostname|port|pathname|search|hash)\b(?!\s*(?:=(?!=)|\+=|-=|\+\+|--))|(?<![\w$.])location\.(href|origin|protocol|host|hostname|port|pathname|search|hash)\b(?!\s*(?:=(?!=)|\+=|-=|\+\+|--))|(?<![\w$.])document\.(URL|documentURI)\b(?!\s*=(?!=))/g;
// Appended to a rewritten script. (A function declaration is hoisted, and being at the END it
// can't break a leading "use strict".) In pages the stand-in is made by the injected script;
// in workers it is built from the worker script's own address.
const PL_TAIL =
  '\n;function __PLmk(){var u;try{u=new URL(new URL(location.href).searchParams.get("url"))}catch(e){return location}' +
  'var o={};["href","origin","protocol","host","hostname","port","pathname","search","hash"].forEach(function(k){' +
  "Object.defineProperty(o,k,{get:function(){return u[k]}})});o.toString=function(){return u.href};return self.__PL=o}\n";
function rewriteJs(text, where) {
  if (text.indexOf("location") === -1 && text.indexOf("document.URL") === -1 && text.indexOf("documentURI") === -1) return null;
  if (text.indexOf("\uFFFD") !== -1) return null; // not really text (e.g. still compressed): leave it alone
  let n = 0;
  const out = text.replace(LOC_RE, (m, p1, p2, p3) => {
    n++;
    return "(self.__PL||__PLmk())." + (p3 ? (p3 === "URL" || p3 === "documentURI" ? "href" : p3) : p1 || p2);
  });
  if (where && /Unauthorized host|unauthorised host/i.test(text)) logDiag(where.split(" ")[0], "info", "SITE-LOCK TEXT in " + where.split(" ")[1] + "; rewrote " + n + " location reads", "");
  return n ? out + PL_TAIL : null;
}

function absolutize(u, base) {
  u = (u || "").trim();
  if (!u || /^(data:|blob:|javascript:|about:|mailto:|tel:|#)/i.test(u)) return null;
  try {
    return new URL(u, base).href;
  } catch {
    return null;
  }
}

// HTML attributes can contain entities (&amp; &#x3D; &quot; ...). The browser decodes them
// before using the URL, so we must too.
function decodeEntities(s) {
  return s.replace(/&(#x[0-9a-f]+|#\d+|amp|quot|apos|lt|gt);/gi, (m, e) => {
    e = e.toLowerCase();
    if (e === "amp") return "&";
    if (e === "quot") return '"';
    if (e === "apos") return "'";
    if (e === "lt") return "<";
    if (e === "gt") return ">";
    const n = e[1] === "x" ? parseInt(e.slice(2), 16) : parseInt(e.slice(1), 10);
    try {
      return String.fromCodePoint(n);
    } catch {
      return m;
    }
  });
}

function rwUrl(u, base) {
  if (u.startsWith("/go?url=")) return u;
  const a = absolutize(decodeEntities(u), base);
  return a ? prox(a) : u;
}

function rewriteCss(css, base) {
  return css
    .replace(/url\(\s*(['"]?)(.*?)\1\s*\)/gi, (m, q, u) => `url(${q}${rwUrl(u, base)}${q})`)
    .replace(/@import\s+(['"])(.*?)\1/gi, (m, q, u) => `@import ${q}${rwUrl(u, base)}${q}`);
}

function rewriteSrcset(v, base) {
  // URLs can contain commas (e.g. "osm-intl,a,a,a,250x200.png"), so only split on a comma
  // that follows a "1x"/"300w" descriptor or is followed by whitespace.
  return v
    .split(/(?<=\s[\d.]+[wx])\s*,\s*|,\s+/)
    .map((part) => {
      const [u, ...rest] = part.trim().split(/\s+/);
      return [rwUrl(u, base), ...rest].join(" ");
    })
    .join(", ");
}

// Script injected into every page. Fixes things the server can't see
// (fetch, XHR, dynamically added images/scripts, forms).
const CLIENT = `(function(){
var B=window.__PBASE;
var O=location.origin+"/go?url=";
var of=window.fetch;
var RC=0;
function toParent(m){try{if(window.__PPROBE&&window.parent!==window)window.parent.postMessage(m,"*");}catch(e){}}
function rep(type,msg){
  if(RC++>60)return;
  toParent({__proxy:"diag",type:type,msg:String(msg).slice(0,300)});
  if(!of)return;
  try{of.call(window,location.origin+"/__diag",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({page:B,type:type,msg:String(msg).slice(0,300)}),keepalive:true});}catch(e){}
}
function rw(u){
  if(u==null)return u; u=String(u);
  if(u.trim()==="")return u;
  if(/^(data:|blob:|javascript:|about:|mailto:|tel:|#)/i.test(u)||u.indexOf("/go?url=")===0||u.indexOf(O)===0)return u;
  try{
    var a=new URL(u,B);
    if(a.origin===location.origin){
      var p=a.pathname;
      if(p==="/__diag"||p==="/__cookie"||p==="/ws"||p==="/test")return u;
      a=new URL(p+a.search+a.hash,B);
    }
    return "/go?url="+encodeURIComponent(a.href);
  }catch(e){return u;}
}
// Stand-in for "location" that answers with the REAL site's address (scripts are rewritten to read this)
function cur(){try{var p=new URL(location.href).searchParams.get("url");if(p)return new URL(p);}catch(e){}return new URL(B);}
var PL={};
["href","origin","protocol","host","hostname","port","pathname","search"].forEach(function(k){
  Object.defineProperty(PL,k,{get:function(){var c=cur();return k==="href"?c.href.split("#")[0]+location.hash:c[k];}});
});
Object.defineProperty(PL,"hash",{get:function(){return location.hash;}});
PL.toString=function(){return PL.href;};
try{Object.defineProperty(window,"__PL",{value:PL,configurable:true});}catch(e){window.__PL=PL;}
/*COOKIE-START*/
var CJ={};
String(window.__PCOOKIES||"").split("; ").forEach(function(c){var i=c.indexOf("=");if(i>0)CJ[c.slice(0,i)]=c.slice(i+1);});
function cookieSet(str){
  var parts=String(str).split(";");var first=parts[0];var i=first.indexOf("=");
  if(i<1)return false;
  var name=first.slice(0,i).trim(),val=first.slice(i+1).trim();
  var dead=false;
  for(var k=1;k<parts.length;k++){
    var a=parts[k].trim();var al=a.toLowerCase();
    if(al.indexOf("max-age=")===0&&parseInt(al.slice(8),10)<=0)dead=true;
    if(al.indexOf("expires=")===0){var t=Date.parse(a.slice(8));if(!isNaN(t)&&t<Date.now())dead=true;}
  }
  if(dead)delete CJ[name];else CJ[name]=val;
  return true;
}
function cookieGet(){return Object.keys(CJ).map(function(k){return k+"="+CJ[k];}).join("; ");}
/*COOKIE-END*/
try{
  Object.defineProperty(Document.prototype,"cookie",{configurable:true,get:cookieGet,set:function(v){
    if(cookieSet(v)&&of){try{of.call(window,location.origin+"/__cookie",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({page:B,cookie:String(v)}),keepalive:true});}catch(e){}}
  }});
}catch(e){}
window.addEventListener("error",function(e){
  var t=e.target;
  if(t&&t!==window&&(t.src||t.href))rep("resource-failed",t.src||t.href);
  else rep("js-error",(e.message||"")+" @"+String(e.filename||"").slice(-80));
},true);
window.addEventListener("unhandledrejection",function(e){
  var r=e.reason;rep("promise-rejected",r==null?"(no reason given)":((r&&(r.stack||r.message))||String(r)));
});
["error","warn"].forEach(function(n){
  var oc=console[n];
  console[n]=function(){
    try{rep("console-"+n,Array.prototype.slice.call(arguments).map(function(a){return a&&a.stack?a.stack:(typeof a==="object"?JSON.stringify(a):String(a));}).join(" ").slice(0,300));}catch(x){}
    return oc.apply(this,arguments);
  };
});
/*FETCH-START*/
if(of)window.fetch=function(i,o){
  var self=this,shown=i;
  var fail=function(err){rep("fetch-failed",((shown&&shown.url)||shown)+" "+err);throw err;};
  var chk=function(r){if(r&&r.status>=400)rep("http-"+r.status,String((shown&&shown.url)||shown).split("?")[0]);return r;};
  try{
    if(typeof i==="string"||i instanceof URL)return of.call(self,rw(i),o).then(chk,fail);
    if(i&&i.url){
      var init={method:i.method,headers:i.headers,credentials:i.credentials,cache:i.cache,redirect:i.redirect,signal:i.signal};
      var url=rw(i.url);
      var go=function(body){
        if(body&&body.byteLength)init.body=body;
        return of.call(self,url,Object.assign(init,o||{})).then(chk,fail);
      };
      return (i.method==="GET"||i.method==="HEAD")?go():i.clone().arrayBuffer().then(go);
    }
  }catch(e){}
  return of.call(self,i,o).then(chk,fail);
};
/*FETCH-END*/
var ox=XMLHttpRequest.prototype.open;
XMLHttpRequest.prototype.open=function(m,u){
  var shown=u;
  var xh=this;
  try{
    xh.addEventListener("error",function(){rep("xhr-failed",shown);});
    xh.addEventListener("load",function(){if(xh.status>=400)rep("http-"+xh.status,String(shown).split("?")[0]);});
  }catch(e){}
  arguments[1]=rw(u);return ox.apply(this,arguments);
};
var ow=window.open;
window.open=function(u){if(u)arguments[0]=rw(u);return ow.apply(this,arguments);};
var OW=window.WebSocket;
if(OW)window.WebSocket=new Proxy(OW,{construct:function(t,a){
  var shown=a[0];
  try{
    var u=new URL(String(a[0]),B);
    if(u.host===location.host&&u.pathname!=="/ws")u=new URL(u.pathname+u.search,B);
    if(u.protocol==="http:")u.protocol="ws:";
    else if(u.protocol==="https:")u.protocol="wss:";
    if(!(u.host===location.host&&u.pathname==="/ws"))
      a[0]=(location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws?url="+encodeURIComponent(u.href)+"&origin="+encodeURIComponent(new URL(B).origin);
  }catch(e){}
  var w=Reflect.construct(t,a);
  try{
    w.addEventListener("error",function(){rep("ws-error",shown);});
    w.addEventListener("close",function(e){if(e.code!==1000&&e.code!==1005)rep("ws-closed",shown+" code "+e.code);});
  }catch(e){}
  return w;
}});
["Worker","SharedWorker","EventSource"].forEach(function(n){
  var O2=window[n];
  if(O2)window[n]=new Proxy(O2,{construct:function(t,a){try{a[0]=rw(a[0]);}catch(e){}return Reflect.construct(t,a);}});
});
if(navigator.sendBeacon){
  var sb=navigator.sendBeacon;
  navigator.sendBeacon=function(u,d){return sb.call(this,rw(u),d);};
}
["pushState","replaceState"].forEach(function(n){
  var o=history[n];
  history[n]=function(s,t,u){if(u!=null&&u!=="")u=rw(u);return o.call(this,s,t,u);};
});
try{
  if(navigator.serviceWorker)navigator.serviceWorker.register=function(){
    // Pretend the registration worked (but nothing ever activates). Games that wait on
    // register() keep going instead of crashing; ones that need a worker still fail visibly.
    var sc=location.origin+"/";
    return Promise.resolve({scope:sc,installing:null,waiting:null,active:null,
      update:function(){return Promise.resolve();},
      unregister:function(){return Promise.resolve(true);},
      addEventListener:function(){},removeEventListener:function(){}});
  };
}catch(e){}
var sa=Element.prototype.setAttribute;
Element.prototype.setAttribute=function(n,v){
  if(/^(src|href|poster|data-src)$/i.test(n)&&this.tagName!=="FORM")v=rw(v);
  return sa.call(this,n,v);
};
[[HTMLImageElement,"src"],[HTMLScriptElement,"src"],[HTMLLinkElement,"href"],[HTMLAnchorElement,"href"],
 [HTMLIFrameElement,"src"],[HTMLSourceElement,"src"],[HTMLMediaElement,"src"]].forEach(function(p){
  var d=Object.getOwnPropertyDescriptor(p[0].prototype,p[1]);
  if(!d||!d.set)return;
  Object.defineProperty(p[0].prototype,p[1],{get:d.get,set:function(v){d.set.call(this,rw(v));},configurable:true,enumerable:d.enumerable});
});
function fix(n){
  ["src","href"].forEach(function(a){
    var v=n.getAttribute&&n.getAttribute(a);
    if(v){var r=rw(v);if(r!==v)sa.call(n,a,r);}
  });
}
new MutationObserver(function(ms){
  ms.forEach(function(m){m.addedNodes.forEach(function(n){
    if(n.nodeType!==1)return;
    fix(n);
    if(n.querySelectorAll)n.querySelectorAll("[src],[href]").forEach(fix);
  });});
}).observe(document,{childList:true,subtree:true});
document.addEventListener("submit",function(e){
  var f=e.target;
  if(!f||f.tagName!=="FORM")return;
  var act=f.getAttribute("action")||"";
  var method=(f.getAttribute("method")||"get").toLowerCase();
  if(act.indexOf("/go?url=")===0)return;
  if(method==="post"){sa.call(f,"action",rw(act||B));return;}
  e.preventDefault();
  var u;try{u=new URL(act||B,B);}catch(x){return;}
  u.search="";
  new FormData(f).forEach(function(v,k){if(typeof v==="string")u.searchParams.append(k,v);});
  location.href=rw(u.href);
},true);
/* ---------- password manager (save-on-submit + click-to-fill ONLY; no keystroke capture) ---------- */
(function(){
function siteOf(){try{return new URL(cur().href).hostname.split(".").slice(-2).join(".");}catch(e){return location.hostname;}}
function findPwField(form){return form.querySelector('input[type="password"]');}
function findUserField(form,pwEl){
  var cands=form.querySelectorAll('input[type="text"],input[type="email"],input:not([type])');
  for(var i=cands.length-1;i>=0;i--){if(cands[i].compareDocumentPosition(pwEl)&Node.DOCUMENT_POSITION_FOLLOWING)return cands[i];}
  return cands[0]||null;
}
function banner(msg,onYes){
  var old=document.getElementById("__pwbar");if(old)old.remove();
  var b=document.createElement("div");b.id="__pwbar";
  b.style.cssText="position:fixed;left:0;right:0;bottom:0;z-index:2147483647;background:#1b1b2a;color:#eee;font:14px system-ui,sans-serif;padding:12px 16px;display:flex;gap:10px;align-items:center;box-shadow:0 -2px 10px rgba(0,0,0,.4)";
  var sp=document.createElement("span");sp.textContent=msg;sp.style.flex="1";b.appendChild(sp);
  var y=document.createElement("button");y.textContent="Save";y.style.cssText="padding:7px 14px;border:0;border-radius:7px;background:#7c6cff;color:#fff;cursor:pointer";
  var n=document.createElement("button");n.textContent="Not now";n.style.cssText="padding:7px 14px;border:0;border-radius:7px;background:#333;color:#ccc;cursor:pointer";
  y.onclick=function(){onYes();b.remove();};n.onclick=function(){b.remove();};
  b.appendChild(y);b.appendChild(n);
  (document.body||document.documentElement).appendChild(b);
}
// The form submit navigates the page away immediately, so we stash the candidate
// (this tab only, cleared after use) and show the "save?" banner on the NEXT page load -
// this is also how real browser password managers do it.
document.addEventListener("submit",function(e){
  var form=e.target;if(!form||form.tagName!=="FORM")return;
  var pw=findPwField(form);if(!pw||!pw.value)return;
  var user=findUserField(form,pw);var username=(user&&user.value)||"";
  if(!username)return;
  try{sessionStorage.setItem("__pwPending",JSON.stringify({site:siteOf(),username:username,password:pw.value}));}catch(e2){}
},true);
(function(){
  var raw;try{raw=sessionStorage.getItem("__pwPending");}catch(e){raw=null;}
  if(!raw)return;
  try{sessionStorage.removeItem("__pwPending");}catch(e){}
  var cand;try{cand=JSON.parse(raw);}catch(e){return;}
  if(!cand||!cand.site||!cand.username||!cand.password)return;
  if(cand.site!==siteOf())return; // only offer to save after landing back on the SAME site
  var run=function(){
    banner('Save this password for '+cand.site+'?',function(){
      of.call(window,location.origin+"/__pw/save",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify(cand)}).catch(function(){});
    });
  };
  if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",run);else run();
})();
function addFillButtons(){
  document.querySelectorAll('input[type="password"]').forEach(function(pw){
    if(pw.__pwBtnAdded)return;pw.__pwBtnAdded=true;
    var form=pw.closest("form");if(!form)return;
    of.call(window,location.origin+"/__pw/for?site="+encodeURIComponent(siteOf())).then(function(r){return r.json();}).then(function(list){
      if(!list||!list.length||pw.__pwBtnAdded===2)return;pw.__pwBtnAdded=2;
      var btn=document.createElement("button");btn.type="button";btn.textContent="\uD83D\uDD11 fill saved login";
      btn.style.cssText="margin:4px 0;padding:5px 10px;font-size:12px;border:1px solid #7c6cff;border-radius:6px;background:#fff;color:#7c6cff;cursor:pointer";
      btn.onclick=function(ev){
        ev.preventDefault();
        var id=list.length===1?list[0].id:prompt("Multiple saved logins for this site. Username to use:\\n"+list.map(function(x){return x.username;}).join(", "));
        var entry=list.find(function(x){return x.id===id;})||list[0];
        of.call(window,location.origin+"/__pw/fill",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({id:entry.id})}).then(function(r){return r.json();}).then(function(d){
          if(d.error)return;
          var user=findUserField(form,pw);
          if(user){user.value=d.username;user.dispatchEvent(new Event("input",{bubbles:true}));}
          pw.value=d.password;pw.dispatchEvent(new Event("input",{bubbles:true}));
        });
      };
      pw.parentNode.insertBefore(btn,pw.nextSibling);
    }).catch(function(){});
  });
}
new MutationObserver(addFillButtons).observe(document.documentElement,{childList:true,subtree:true});
document.addEventListener("DOMContentLoaded",addFillButtons);
setTimeout(addFillButtons,800);
})();
toParent({__proxy:"start"});
document.addEventListener("DOMContentLoaded",function(){toParent({__proxy:"dom-ready"});});
window.addEventListener("load",function(){
  toParent({__proxy:"load"});
  try{window.__PLOADMS=Math.round(performance.now());}catch(e){}
  setTimeout(function(){
    try{toParent({__proxy:"snapshot",title:document.title,text:((document.body&&document.body.innerText)||"").length,canvas:document.getElementsByTagName("canvas").length,imgs:document.images.length});}catch(e){}
    // Auto-retry: if the page looks blank, ask the proxy to loosen its speedups for THIS
    // site only, remember that, and reload once (sessionStorage stops it retrying forever).
    try{
      var blank=((document.body&&document.body.innerText)||"").trim().length<30&&!document.getElementsByTagName("canvas").length&&!document.images.length;
      var key="__pretried:"+B;
      if(blank&&!sessionStorage.getItem(key)){
        sessionStorage.setItem(key,"1");
        of.call(window,location.origin+"/__autoretry",{method:"POST",headers:{"content-type":"application/json"},body:JSON.stringify({site:B})}).then(function(r){return r.json();}).then(function(d){
          if(d&&d.fix)location.reload();
        }).catch(function(){});
      }
    }catch(e){}
  },3000);
});
})();`;

function rewriteHtml(html, base, probe) {
  const bm = html.match(/<base\s[^>]*href\s*=\s*["']([^"']+)["'][^>]*>/i);
  if (bm) {
    base = absolutize(bm[1], base) || base;
    html = html.replace(bm[0], "");
  }
  for (const sm of html.matchAll(/<script\b[^>]*?\ssrc\s*=\s*["']([^"']+)["']/gi)) {
    const abs = absolutize(decodeEntities(sm[1]), base);
    if (abs) noteScriptDir(siteKey(base), abs);
  }
  html = html.replace(/\sintegrity\s*=\s*(["']).*?\1/gi, "");
  const doInline = REWRITE_JS && !NO_JS_REWRITE.test(new URL(base).hostname);
  // Set aside the BODIES of <script> and <style> before the address-rewriting passes below run.
  // Those passes look for things like src="..." anywhere in the text - without this, ordinary
  // JavaScript such as  '<img src="'+x+'">'  gets "rewritten" into broken code and the whole
  // script dies. This silently broke any page/game whose own script happened to contain that text.
  const rawBlocks = [];
  html = html.replace(/(<(script|style)\b[^>]*>)([\s\S]*?)(<\/\2\s*>)/gi, (m, open, tag, content, close) => {
    rawBlocks.push({ tag: tag.toLowerCase(), open, content });
    return open + "" + (rawBlocks.length - 1) + "" + close;
  });
  html = html.replace(
    /(\s(?:href|src|poster|data-src|data-href)\s*=\s*)(["'])(.*?)\2/gi,
    (m, p, q, u) => p + q + rwUrl(u, base) + q
  );
  html = html.replace(
    /(\ssrcset\s*=\s*)(["'])(.*?)\2/gi,
    (m, p, q, v) => p + q + rewriteSrcset(v, base) + q
  );
  html = html.replace(
    /(\sstyle\s*=\s*)(["'])(.*?)\2/gi,
    (m, p, q, v) => p + q + rewriteCss(v, base) + q
  );
  // put the script/style bodies back, rewriting each one properly now that it's safe to
  html = html.replace(/(\d+)/g, (m, idx) => {
    const b = rawBlocks[Number(idx)];
    if (!b) return "";
    if (b.tag === "style") return rewriteCss(b.content, base);
    const skip = /\ssrc\s*=/i.test(b.open) || /\stype\s*=\s*["']?(?:application\/(?:ld\+)?json|importmap|speculationrules|text\/template)/i.test(b.open);
    if (!doInline || skip) return b.content;
    const r = rewriteJs(b.content, siteKey(base) + " (inline)");
    return r || b.content;
  });
  html = html.replace(
    /(<meta[^>]*http-equiv\s*=\s*["']refresh["'][^>]*content\s*=\s*["']\s*\d+\s*;\s*url=)([^"']+)/gi,
    (m, a, u) => a + rwUrl(u, base)
  );
  const inject =
    "<script>window.__PBASE=" +
    JSON.stringify(base).replace(/</g, "\\u003c") +
    (probe ? ";window.__PPROBE=true" : "") +
    ";window.__PCOOKIES=" +
    JSON.stringify(cookiesFor(new URL(base).hostname, false)).replace(/</g, "\\u003c") +
    ";</script><script>" +
    CLIENT +
    "</script>";
  if (/<head[^>]*>/i.test(html)) return html.replace(/<head[^>]*>/i, (m) => m + inject);
  return inject + html;
}

function storeCookies(host, setCookies) {
  host = host.toLowerCase();
  for (const sc of setCookies) {
    const parts = sc.split(";");
    const first = parts[0];
    const i = first.indexOf("=");
    if (i < 1) continue;
    const name = first.slice(0, i).trim();
    const value = first.slice(i + 1).trim();
    let domain = host;
    let httpOnly = false;
    let dead = false;
    for (const p of parts.slice(1)) {
      const a = p.trim();
      const al = a.toLowerCase();
      if (al.startsWith("domain=")) {
        const d = al.slice(7).replace(/^\./, "");
        if (d.includes(".") && (host === d || host.endsWith("." + d))) domain = d;
      } else if (al === "httponly") httpOnly = true;
      else if (al.startsWith("max-age=") && parseInt(al.slice(8), 10) <= 0) dead = true;
      else if (al.startsWith("expires=")) {
        const t = Date.parse(a.slice(8));
        if (!isNaN(t) && t < Date.now()) dead = true;
      }
    }
    if (!jar.has(domain)) jar.set(domain, new Map());
    const m = jar.get(domain);
    if (dead || value === "") m.delete(name);
    else m.set(name, { value, httpOnly });
  }
}

// Cookies for a host. Cookies set for ".site.com" are shared by all of its subdomains.
function cookiesFor(host, includeHttpOnly) {
  host = host.toLowerCase();
  const out = new Map();
  for (const [domain, m] of jar) {
    if (host !== domain && !host.endsWith("." + domain)) continue;
    for (const [name, c] of m) {
      if (!includeHttpOnly && c.httpOnly) continue;
      const prev = out.get(name);
      if (!prev || domain.length > prev.len) out.set(name, { value: c.value, len: domain.length });
    }
  }
  return [...out].map(([k, v]) => `${k}=${v.value}`).join("; ");
}

function cookieHeader(host) {
  return cookiesFor(host, true);
}

function guess(input) {
  input = input.trim();
  if (/^https?:\/\//i.test(input)) return input;
  if (!/\s/.test(input) && (/\./.test(input) || /^localhost/i.test(input)))
    return (/^localhost/i.test(input) ? "http://" : "https://") + input;
  return "https://search.brave.com/search?q=" + encodeURIComponent(input);
}

function readBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => resolve(Buffer.concat(chunks)));
  });
}

function decode(buf, ct) {
  const m = /charset\s*=\s*["']?([\w-]+)/i.exec(ct || "");
  try {
    return new TextDecoder(m ? m[1] : "utf-8").decode(buf);
  } catch {
    return new TextDecoder("utf-8").decode(buf);
  }
}

// Some servers (poxel.io's ad/game-script host cdn.cpmstar.com is one) answer Node's built-in
// fetch with "tlsv1 alert internal error". When that happens we retry the request with Node's
// https module and explicit, older-style TLS settings, and remember which hosts needed it.
const TLS_TRIES = [
  { ecdhCurve: "X25519:prime256v1:secp384r1", minVersion: "TLSv1.2" },
  { ecdhCurve: "prime256v1:secp384r1", minVersion: "TLSv1.2", maxVersion: "TLSv1.2" },
];
// Some sites send an incomplete certificate chain (browsers repair that by themselves, Node does not:
// "unable to get local issuer certificate"). For those two specific errors only, we retry without
// the chain check and remember the host. STRICT_TLS=1 turns this off.
const lenientHosts = new Set();
function isChainError(e) {
  const c = String((e.cause && e.cause.code) || e.code || "");
  return c === "UNABLE_TO_GET_ISSUER_CERT_LOCALLY" || c === "UNABLE_TO_VERIFY_LEAF_SIGNATURE";
}
const tlsFallbackHosts = new Map(); // hostname -> index of TLS_TRIES that worked
function isTlsError(e) {
  const m = String((e.cause && e.cause.message) || e.message || "");
  const c = String((e.cause && e.cause.code) || "");
  return /ssl|tls|alert|handshake/i.test(m) || /^ERR_(SSL|TLS)/.test(c);
}
function fetchViaHttps(href, init, tlsOpts) {
  return new Promise((resolve, reject) => {
    const u = new URL(href);
    const isHttps = u.protocol === "https:";
    const opts = { method: init.method, headers: init.headers, signal: init.signal };
    if (isHttps) Object.assign(opts, tlsOpts, { servername: net.isIP(u.hostname) ? undefined : u.hostname });
    const rq = (isHttps ? https : http).request(u, opts, (inc) => {
      const h = new Headers();
      for (let i = 0; i < inc.rawHeaders.length; i += 2) {
        try {
          h.append(inc.rawHeaders[i], inc.rawHeaders[i + 1]);
        } catch {}
      }
      const noBody = [204, 205, 304].includes(inc.statusCode) || init.method === "HEAD";
      if (noBody) inc.resume();
      resolve(new Response(noBody ? null : Readable.toWeb(inc), { status: inc.statusCode, headers: h }));
    });
    rq.on("error", reject);
    if (init.body && init.body.length) rq.write(init.body);
    rq.end();
  });
}

// ---- Ad blocking (like an ad blocker): ad scripts often crash or hang when they run on a
// different address (e.g. Google's IMA SDK refuses to run unless it is on a Google domain),
// and games wait forever for them. Failing them fast lets the game carry on, exactly as it
// does for people with ad blockers. Set BLOCK_ADS=0 to turn this off.
let BLOCK_ADS = process.env.BLOCK_ADS !== "0";
const AD_HOSTS = /(^|\.)(imasdk\.googleapis\.com|googlesyndication\.com|doubleclick\.net|googletagservices\.com|googleadservices\.com|adservice\.google\.com|cpmstar\.com|vntsm\.io|adnxs\.com|amazon-adsystem\.com|pubmatic\.com|rubiconproject\.com|openx\.net|criteo\.com|taboola\.com|outbrain\.com|adsrvr\.org|rlcdn\.com|bidswitch\.net|bids\.ws|3lift\.com|casalemedia\.com|adform\.net|33across\.com|yieldmo\.com|smartadserver\.com|media\.net|contextweb\.com|sonobi\.com|inmobi\.com|1rx\.io|lijit\.com|omnitagjs\.com|servenobid\.com|deepintent\.com|tapad\.com|dotomi\.com|sitescout\.com|richaudience\.com|adentifi\.com|getpublica\.com|smaato\.net|iqm\.com|creativecdn\.com|ymmobi\.com|stackadapt\.com|eskimi\.com|ortb\.net|bidr\.io|pinklion\.io|syncingbridge\.com|admanmedia\.com|a-mo\.net|amxrtb\.com|amx1\.net)$/i;

// ---- Code-splitting fix: bundlers (webpack) load extra chunks (4133.js ...) relative to the
// address of the main script. Through the proxy that address is /go?url=..., so chunk requests
// arrive at the wrong place. We remember the folders real scripts came from and, when a
// "lost" request 404s, look for the file in those folders.
const scriptDirs = new Map(); // site -> [folder urls, newest first]
function noteScriptDir(site, href) {
  try {
    const dir = new URL(".", href).href;
    const list = scriptDirs.get(site) || [];
    const i = list.indexOf(dir);
    if (i !== -1) list.splice(i, 1);
    list.unshift(dir);
    if (list.length > 30) list.pop();
    scriptDirs.set(site, list);
  } catch {}
}

// One quick retry for short network hiccups (DNS blips, dropped connections)
async function fetchRetry(href, init) {
  const host = new URL(href).hostname;
  const forced = process.env.TLS_FORCE_FALLBACK === "1"; // for testing only
  if (lenientHosts.has(host)) return await fetchViaHttps(href, init, { ...TLS_TRIES[0], rejectUnauthorized: false });
  if (tlsFallbackHosts.has(host)) {
    try {
      return await fetchViaHttps(href, init, TLS_TRIES[tlsFallbackHosts.get(host)]);
    } catch {
      tlsFallbackHosts.delete(host);
    }
  }
  try {
    if (forced && href.startsWith("https:")) throw Object.assign(new Error("forced"), { cause: { message: "forced tls test" } });
    return await fetch(href, init);
  } catch (e) {
    if (init.signal && init.signal.aborted) throw e;
    if (href.startsWith("https:") && process.env.STRICT_TLS !== "1" && isChainError(e)) {
      const r = await fetchViaHttps(href, init, { ...TLS_TRIES[0], rejectUnauthorized: false });
      lenientHosts.add(host);
      logDiag(host, "info", "incomplete certificate chain - continued without the chain check", href);
      return r;
    }
    if (href.startsWith("https:") && isTlsError(e)) {
      for (let i = 0; i < TLS_TRIES.length; i++) {
        try {
          const r = await fetchViaHttps(href, init, TLS_TRIES[i]);
          tlsFallbackHosts.set(host, i);
          return r;
        } catch {}
      }
      throw e;
    }
    const code = e.cause && e.cause.code;
    const hiccup = ["EAI_AGAIN", "ECONNRESET", "ETIMEDOUT", "UND_ERR_SOCKET", "UND_ERR_CONNECT_TIMEOUT"];
    if ((init.method === "GET" || init.method === "HEAD") && hiccup.includes(code)) {
      await new Promise((r) => setTimeout(r, 300));
      try {
        return await fetch(href, init);
      } catch (e2) {
        if (init.signal && init.signal.aborted) throw e2;
        // last try: a different connection method (Node's https module, IPv4 only)
        try {
          return await fetchViaHttps(href, init, { ...TLS_TRIES[0], family: 4 });
        } catch {
          throw e2;
        }
      }
    }
    throw e;
  }
}

async function handleGo(req, res, url) {
  const __t0 = performance.now();
  const target = guess(url.searchParams.get("url") || "");
  let t;
  try {
    t = new URL(target);
  } catch {
    res.statusCode = 400;
    return res.end("Bad URL");
  }
  const __tm = SLOW_ON ? slowStart(req, res, t, __t0) : null;

  // Which real page is asking? (the browser tells us in the Referer)
  let realRef = null;
  try {
    const rr = new URL(req.headers.referer || "");
    if (rr.pathname === "/go" && rr.searchParams.get("url"))
      realRef = new URL(guess(rr.searchParams.get("url")));
  } catch {}

  // Pages often build addresses from location.origin, which is OUR address.
  // Point those at the real site instead of at ourselves.
  if (t.host === req.headers.host) {
    if (!realRef) {
      res.statusCode = 400;
      return res.end("Bad URL");
    }
    t = new URL(t.pathname + t.search, realRef.href);
  }
  if (!/^https?:$/.test(t.protocol)) {
    res.statusCode = 400;
    return res.end("Only http and https addresses work");
  }
  if (await isBlockedHost(t.hostname)) {
    res.statusCode = 403;
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    return res.end("Blocked: this proxy won't open private or internal addresses.");
  }
  slowMark(__tm, "check");

  // Pass along the browser's own headers (API keys, CSRF tokens, etc.)
  const SKIP = new Set([
    "host", "connection", "content-length", "cookie", "origin", "referer",
    "accept-encoding", "upgrade-insecure-requests", "transfer-encoding",
    "keep-alive", "te", "trailer", "upgrade", "proxy-connection",
    "proxy-authorization", "expect",
    "sec-fetch-site", "forwarded", "x-forwarded-for", "x-forwarded-host",
    "x-forwarded-proto", "via",
  ]);
  const headers = {};
  for (const [k, v] of Object.entries(req.headers)) {
    if (!SKIP.has(k)) headers[k] = v;
  }
  headers["user-agent"] = req.headers["user-agent"] || UA;
  headers.accept = req.headers.accept || "*/*";
  headers["accept-language"] = req.headers["accept-language"] || "en-US,en;q=0.9";

  // Tell the real site which page we came from (many sites check this)
  const pageHost = realRef ? realRef.hostname : t.hostname;
  const siteFix = siteFixGet(siteKey(pageHost));
  const effBlockAds = siteFix.blockAds === false ? false : BLOCK_ADS;
  const effRewriteJs = siteFix.rewriteJs === false ? false : REWRITE_JS;
  if (effBlockAds && AD_HOSTS.test(t.hostname) && req.headers["sec-fetch-dest"] !== "document") {
    logDiag(pageHost, "ad-blocked", "", t.href);
    res.writeHead(403, { "Content-Type": "text/plain" });
    return res.end("blocked");
  }
  headers.referer = realRef ? realRef.href : t.origin + "/";
  const refOrigin = realRef ? realRef.origin : t.origin;
  headers["sec-fetch-site"] =
    refOrigin === t.origin
      ? "same-origin"
      : siteKey(refOrigin) === siteKey(t.origin)
        ? "same-site"
        : "cross-site";
  if (
    (req.method !== "GET" && req.method !== "HEAD") ||
    (refOrigin !== t.origin && req.headers["sec-fetch-mode"] === "cors")
  )
    headers.origin = refOrigin;
  const ck = cookieHeader(t.hostname);
  if (ck) headers.cookie = ck;

  // Served from our own memory if we fetched this file recently and the real site allows caching
  const cacheKey = t.href + "|" + (req.headers.accept || "");
  const cacheable = req.method === "GET" && !req.headers.range && !req.headers.authorization;
  if (cacheable) {
    const hit = cacheGet(cacheKey);
    if (hit) {
      if (/javascript|ecmascript/i.test(hit.headers["content-type"] || "")) noteScriptDir(siteKey(pageHost), t.href);
      if (__tm) __tm.status = hit.status;
      slowMark(__tm, "upstream");
      slowMark(__tm, "processed");
      inspectorNote({ t: Date.now(), method: req.method, host: siteKey(pageHost), url: t.href, path: t.pathname.slice(0, 80), status: hit.status, ms: Math.round(performance.now() - __t0), cache: true });
      return sendBuf(req, res, hit.status, hit.headers, hit.body, hit);
    }
  }

  const ac = new AbortController();
  res.on("close", () => {
    if (!res.writableEnded) ac.abort();
  });
  const init = { method: req.method, headers, redirect: "manual", signal: ac.signal };
  if (req.method !== "GET" && req.method !== "HEAD") {
    init.body = await readBody(req);
    if (req.headers["content-type"]) headers["content-type"] = req.headers["content-type"];
  }

  let r;
  try {
    r = await fetchRetry(t.href, init);
  } catch (e) {
    if (ac.signal.aborted) return;
    logDiag(pageHost, "fetch-error", e.cause?.message || e.message, t.href);
    if (__tm) __tm.status = 502;
    res.statusCode = 502;
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    const code = String((e.cause && e.cause.code) || e.code || "");
    const why = e.cause?.message || e.message;
    const hints = {
      ENOTFOUND: "Your computer/network could not find this website's address (DNS). The site may be blocked by your network or DNS filter, or the address is mistyped.",
      EAI_AGAIN: "The name lookup timed out. Your internet or DNS is slow or blocking it. Try again.",
      ECONNREFUSED: "The site refused the connection.",
      ECONNRESET: "The connection was cut off. A network filter/firewall may be blocking this site.",
      ETIMEDOUT: "The connection timed out. The site may be blocked on your network, or your internet is slow.",
      UND_ERR_CONNECT_TIMEOUT: "Could not connect in time. The site may be blocked on your network, or your internet is slow.",
      CERT_HAS_EXPIRED: "The site's security certificate has expired.",
      ERR_TLS_CERT_ALTNAME_INVALID: "The site's security certificate does not match its address.",
      DEPTH_ZERO_SELF_SIGNED_CERT: "The site uses a self-signed certificate.",
      SELF_SIGNED_CERT_IN_CHAIN: "Your network is replacing security certificates (a school/work filter or antivirus). Pointing the NODE_EXTRA_CA_CERTS setting at that filter's certificate file fixes it.",
    };
    const hint = hints[code] || (isTlsError(e) ? "A secure-connection (TLS) problem with this site." : "");
    return res.end("Could not reach " + t.href + "\n" + why + (code ? "\n(" + code + ")" : "") + (hint ? "\n\n" + hint : "") + "\n\nIf this keeps happening, open /debug and send it.");
  }

  slowMark(__tm, "upstream");
  if (r.status === 404 && req.method === "GET" && /\.js$/i.test(t.pathname) && (url.searchParams.get("lost") === "1" || realRef)) {
    const __lost0 = performance.now();
    const file = t.pathname.split("/").pop() + t.search;
    const mine = siteKey(pageHost);
    const dirs = (scriptDirs.get(mine) || []).slice(0, 15);
    for (const [k, list] of scriptDirs) if (k !== mine) for (const d of list.slice(0, 3)) if (dirs.length < 27 && !dirs.includes(d)) dirs.push(d);
    const cands = [];
    for (const dir of dirs) {
      try {
        const cand = new URL(file, dir);
        if (cand.href !== t.href) cands.push(cand);
      } catch {}
    }
    // SPEED: try several folders AT ONCE instead of one after another
    const hit = await probeFolders(cands, init, headers, realRef, ac.signal);
    if (hit) {
      logDiag(pageHost, "info", "found " + t.pathname + " in " + new URL(".", hit.cand.href).href, hit.cand.href);
      noteScriptDir(siteKey(pageHost), hit.cand.href);
      t = hit.cand;
      r = hit.r2;
    } else {
      logDiag(pageHost, "info", "no known script folder had " + file + " (tried " + cands.length + ")", t.href);
    }
    if (__tm) __tm.lost = Math.round(performance.now() - __lost0);
  }
  if (r.status === 403 && req.headers["sec-fetch-dest"] === "document") {
    const peek = await r.clone().text().catch(() => "");
    if (looksBlocked(403, peek)) {
      logDiag(pageHost, "cloudflare-challenge", "", t.href);
      res.writeHead(403, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
      if (__tm) __tm.status = 403;
      return res.end(challengePage(t));
    }
  }
  if (r.status >= 400) logDiag(pageHost, "http-" + r.status, "", t.href);
  if (__tm) __tm.status = r.status;
  if (r.status >= 200 && r.status < 300 && /javascript|ecmascript/i.test(r.headers.get("content-type") || ""))
    noteScriptDir(siteKey(pageHost), t.href);
  storeCookies(t.hostname, r.headers.getSetCookie ? r.headers.getSetCookie() : []);

  // Redirects: send the browser to the proxied version of the new address
  const loc = r.headers.get("location");
  if (r.status >= 300 && r.status < 400 && loc) {
    const next = absolutize(loc, t.href);
    let to = next ? prox(next) : loc;
    if (next && url.searchParams.get("probe") === "1") to += "&probe=1";
    res.writeHead(r.status, { Location: to });
    return res.end();
  }

  const ct = r.headers.get("content-type") || "";
  const out = {};
  for (const h of ["content-type", "content-range", "accept-ranges", "content-disposition", "cache-control", "etag", "last-modified", "expires", "cross-origin-opener-policy", "cross-origin-embedder-policy"]) {
    const v = r.headers.get(h);
    if (v) out[h] = v;
  }

  const isHtml = /text\/html|application\/xhtml/i.test(ct);
  const isCss = /text\/css/i.test(ct);

  const ttl = cacheable ? cacheTtl(req, r, ct) : 0;

  if (isHtml || isCss) {
    const buf = Buffer.from(await r.arrayBuffer());
    const text = decode(buf, ct);
    const body = isHtml
      ? rewriteHtml(text, t.href, url.searchParams.get("probe") === "1")
      : rewriteCss(text, t.href);
    out["content-type"] = (isHtml ? "text/html" : "text/css") + "; charset=utf-8";
    if (isHtml) {
      // HTML carries your cookies inside it, so it must never be cached
      out["cache-control"] = "no-store";
      delete out.etag;
      delete out["last-modified"];
      delete out.expires;
    }
    if (out["cross-origin-opener-policy"] || out["cross-origin-embedder-policy"])
      logDiag(pageHost, "info", "page sets isolation headers: coop=" + (out["cross-origin-opener-policy"] || "-") + " coep=" + (out["cross-origin-embedder-policy"] || "-"), t.href);
    const bodyBuf = Buffer.from(body, "utf8");
    slowMark(__tm, "processed");
    inspectorNote({ t: Date.now(), method: req.method, host: siteKey(pageHost), url: t.href, path: t.pathname.slice(0, 80), status: r.status, ms: Math.round(performance.now() - __t0), cache: false, enc: isHtml ? "html" : "css" });
    if (isCss && ttl) {
      const e = { status: r.status, headers: out, body: bodyBuf, exp: Date.now() + ttl * 1000 };
      cachePut(cacheKey, e);
      return sendBuf(req, res, r.status, out, bodyBuf, e);
    }
    return sendBuf(req, res, r.status, out, bodyBuf);
  }

  // Scripts: swap location.hostname etc. for the real site's address (see rewriteJs)
  const isJs = /javascript|ecmascript/i.test(ct) || /\.m?js(\.br|\.gz|\.unityweb)?$/i.test(t.pathname);
  if (effRewriteJs && isJs && !NO_JS_REWRITE.test(pageHost) && r.status === 200 && req.method === "GET" && Number(r.headers.get("content-length") || 0) <= JS_MAX) {
    const buf = Buffer.from(await r.arrayBuffer());
    const rewritten = buf.length <= JS_MAX ? rewriteJs(decode(buf, ct), pageHost + " " + t.pathname.split("/").pop()) : null;
    let bodyBuf = buf;
    if (rewritten) {
      bodyBuf = Buffer.from(rewritten, "utf8");
      out["content-type"] = (ct.split(";")[0] || "application/javascript") + "; charset=utf-8";
    }
    // Always ask the proxy before reusing a script (cheap: answered "304 unchanged" from memory).
    // Otherwise the browser reuses old copies without telling us, which breaks lost-file lookups
    // and keeps scripts rewritten by an older version.
    out.etag = '"' + crypto.createHash("sha1").update(bodyBuf).digest("hex").slice(0, 20) + '"';
    out["cache-control"] = "no-cache";
    delete out["last-modified"];
    delete out.expires;
    slowMark(__tm, "processed");
    inspectorNote({ t: Date.now(), method: req.method, host: siteKey(pageHost), url: t.href, path: t.pathname.slice(0, 80), status: 200, ms: Math.round(performance.now() - __t0), cache: false, rewritten: !!rewritten, enc: "js" });
    if (ttl && bodyBuf.length <= CACHE_ITEM_MAX) {
      const e = { status: 200, headers: out, body: bodyBuf, exp: Date.now() + ttl * 1000 };
      cachePut(cacheKey, e);
      return sendBuf(req, res, 200, out, bodyBuf, e);
    }
    return sendBuf(req, res, 200, out, bodyBuf);
  }

  // Everything else (images, video, fonts, game data...) streams straight through
  if (!r.body || req.method === "HEAD") {
    res.writeHead(r.status, out);
    return res.end();
  }
  const src = Readable.fromWeb(r.body);
  const rawHeaders = { ...out };
  if (canCompress(r.status, out) && out["content-type"]) out.vary = "Accept-Encoding";
  const enc = canCompress(r.status, rawHeaders) ? pickEncoding(req) : null;
  if (enc) out["content-encoding"] = enc;
  inspectorNote({ t: Date.now(), method: req.method, host: siteKey(pageHost), url: t.href, path: t.pathname.slice(0, 80), status: r.status, ms: Math.round(performance.now() - __t0), cache: false, enc: enc || "" });
  res.writeHead(r.status, out);
  // keep a copy for the cache while it streams to the browser
  if (ttl) {
    let chunks = [];
    let size = 0;
    src.on("data", (c) => {
      if (!chunks) return;
      size += c.length;
      if (size > CACHE_ITEM_MAX) chunks = null;
      else chunks.push(c);
    });
    src.on("end", () => {
      if (chunks) cachePut(cacheKey, { status: r.status, headers: rawHeaders, body: Buffer.concat(chunks), exp: Date.now() + ttl * 1000 });
    });
  }
  pipeline(...(enc ? [src, makeCompressor(enc), res] : [src, res]), () => {});
}

// Built-in WebSocket echo server (used by the tester to check that your host allows WebSockets)
const WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
function wsEcho(req, socket) {
  const key = req.headers["sec-websocket-key"];
  if (!key) return socket.destroy();
  const accept = crypto.createHash("sha1").update(key + WS_GUID).digest("base64");
  socket.write(
    "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
      "Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
  );
  let buf = Buffer.alloc(0);
  const send = (op, payload) => {
    const len = payload.length;
    let head;
    if (len < 126) head = Buffer.from([0x80 | op, len]);
    else if (len < 65536) {
      head = Buffer.alloc(4);
      head[0] = 0x80 | op;
      head[1] = 126;
      head.writeUInt16BE(len, 2);
    } else {
      head = Buffer.alloc(10);
      head[0] = 0x80 | op;
      head[1] = 127;
      head.writeBigUInt64BE(BigInt(len), 2);
    }
    socket.write(Buffer.concat([head, payload]));
  };
  socket.on("data", (d) => {
    buf = Buffer.concat([buf, d]);
    while (buf.length >= 2) {
      const op = buf[0] & 0x0f;
      const masked = (buf[1] & 0x80) !== 0;
      let len = buf[1] & 0x7f;
      let off = 2;
      if (len === 126) {
        if (buf.length < 4) return;
        len = buf.readUInt16BE(2);
        off = 4;
      } else if (len === 127) {
        if (buf.length < 10) return;
        len = Number(buf.readBigUInt64BE(2));
        off = 10;
      }
      if (len > 1 << 20) return socket.destroy();
      const need = off + (masked ? 4 : 0) + len;
      if (buf.length < need) return;
      let payload = buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) {
        const m = buf.subarray(off, off + 4);
        payload = Buffer.from(payload.map((b, i) => b ^ m[i % 4]));
      } else payload = Buffer.from(payload);
      buf = buf.subarray(need);
      if (op === 8) {
        send(8, payload);
        return socket.end();
      }
      if (op === 9) send(10, payload);
      else if (op === 1 || op === 2) send(op, payload);
    }
  });
  socket.on("error", () => {});
}

// Checks run FROM the server (what can your host reach?). Shown by the tester page.
async function selfTest() {
  const checks = [];
  checks.push({ name: "Node.js version", status: "info", detail: process.version + " on " + process.platform });
  checks.push({ name: "Server uptime", status: "info", detail: Math.round(process.uptime()) + " seconds" });
  const probe = async (name, url) => {
    const t0 = Date.now();
    try {
      const r = await fetchRetry(url, {
        method: "GET",
        redirect: "manual",
        signal: AbortSignal.timeout(8000),
        headers: { "user-agent": UA },
      });
      const ms = Date.now() - t0;
      if (r.status < 400) return { name, status: "pass", detail: "HTTP " + r.status + " in " + ms + " ms" };
      return {
        name,
        status: "warn",
        detail: "HTTP " + r.status + " in " + ms + " ms (the site may be blocking this server's IP address)",
      };
    } catch (e) {
      return { name, status: "fail", detail: (e.cause && e.cause.message) || e.message };
    }
  };
  const results = await Promise.all([
    probe("Server can reach example.com", "https://example.com"),
    probe("Server can reach youtube.com", "https://www.youtube.com"),
    probe("Server can reach poxel.io", "https://poxel.io"),
    probe("Server can reach cdn.cpmstar.com (Poxel's game script host)", "https://cdn.cpmstar.com/"),
    probe("Server can reach slither.io", "https://slither.io"),
    (async () => {
      try {
        const addrs = await Promise.race([
          dns.promises.resolve6("ipv6.google.com"),
          new Promise((_, rej) => setTimeout(() => rej(new Error("dns timeout")), 5000)),
        ]);
        const ok = await new Promise((resolve) => {
          const c = net.connect({ host: addrs[0], port: 443, timeout: 4000 }, () => {
            c.destroy();
            resolve(true);
          });
          c.on("error", () => resolve(false));
          c.on("timeout", () => {
            c.destroy();
            resolve(false);
          });
        });
        return ok
          ? { name: "Server has IPv6", status: "pass", detail: "Yes" }
          : { name: "Server has IPv6", status: "warn", detail: "No - games that use IPv6 servers (like Slither.io) will fail here" };
      } catch (e) {
        return { name: "Server has IPv6", status: "warn", detail: "No - games that use IPv6 servers (like Slither.io) will fail here" };
      }
    })(),
  ]);
  return { ok: true, port: PORT, time: Date.now(), checks: checks.concat(results) };
}

function lostRequestTarget(req) {
  try {
    const r = new URL(req.headers.referer || "");
    if (r.pathname !== "/go") return null;
    const orig = r.searchParams.get("url");
    if (!orig) return null;
    return new URL(req.url, guess(orig)).href;
  } catch {
    return null;
  }
}

// WebSocket relay: the browser connects to /ws?url=wss://real-site/..., and we
// open a connection to the real site and pipe the raw bytes both ways.
function handleUpgrade(req, socket, head) {
  try {
    const u = new URL(req.url, `http://localhost:${PORT}`);
    if (u.pathname === "/__echo") return wsEcho(req, socket);
    if (u.pathname !== "/ws") return socket.destroy();
    const t = new URL(u.searchParams.get("url"));
    let pageOrigin = null;
    try {
      const po = new URL(u.searchParams.get("origin") || "");
      if (/^https?:$/.test(po.protocol)) pageOrigin = po.origin;
    } catch {}
    const secure = t.protocol === "wss:" || t.protocol === "https:";
    const port = Number(t.port) || (secure ? 443 : 80);
    const host = t.hostname.replace(/^\[|\]$/g, "");
    const selfEcho = /^(127\.0\.0\.1|localhost)$/.test(host) && port === Number(PORT) && t.pathname === "/__echo";
    if (!selfEcho) {
      isBlockedHost(host).then((blocked) => {
        if (blocked) {
          logDiag(pageOrigin || t.href, "ws-blocked", "private address", t.href);
          return socket.destroy();
        }
        openRelay();
      });
    } else openRelay();
    function openRelay() {
    const up = secure
      ? tls.connect({ host, port, servername: net.isIP(host) ? undefined : host, ecdhCurve: TLS_TRIES[0].ecdhCurve, minVersion: "TLSv1.2", rejectUnauthorized: !lenientHosts.has(host) })
      : net.connect(port, host);
    const wsSite = pageOrigin || t.href;
    const wsStarted = Date.now();
    logDiag(wsSite, "ws-try", "connecting", t.href);
    up.on("error", (e) => {
      logDiag(wsSite, "ws-connect-error", e.code || e.message, t.href);
      socket.destroy();
    });
    up.once("data", (d) => {
      // The game server's first reply line: 101 = accepted, anything else = rejected
      logDiag(wsSite, "ws-server-reply", d.toString("latin1").split("\r\n")[0] + " after " + (Date.now() - wsStarted) + "ms", t.href);
    });
    // SPEED: send small game packets immediately instead of waiting to batch them (less lag)
    socket.setNoDelay(true);
    up.setNoDelay(true);
    socket.on("error", () => up.destroy());
    socket.on("close", () => up.destroy());
    up.on("close", () => socket.destroy());
    up.on(secure ? "secureConnect" : "connect", () => {
      const lines = [`GET ${t.pathname}${t.search} HTTP/1.1`, `Host: ${t.host}`];
      for (const h of [
        "upgrade",
        "connection",
        "sec-websocket-key",
        "sec-websocket-version",
        "sec-websocket-protocol",
        "sec-websocket-extensions",
      ])
        if (req.headers[h]) lines.push(`${h}: ${req.headers[h]}`);
      // Game servers often check Origin: it must be the REAL page's site (e.g. https://slither.io),
      // not the game server's own address.
      lines.push(`Origin: ${pageOrigin || (secure ? "https" : "http") + "://" + t.host}`);
      lines.push(`User-Agent: ${req.headers["user-agent"] || UA}`);
      const ck = cookieHeader(t.hostname);
      if (ck) lines.push(`Cookie: ${ck}`);
      up.write(lines.join("\r\n") + "\r\n\r\n");
      if (head && head.length) up.write(head);
      up.pipe(socket);
      socket.pipe(up);
    });
    }
  } catch (e) {
    noteError(`websocket ${req.url}`, e);
    try { socket.destroy(); } catch {}
  }
}

// The tester pages are built right into this file (no extra files needed).
const TESTER_HTML = "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<title>Proxy Tester</title>\n<style>\n:root{--bg:#0e0e14;--card:#181822;--line:#2a2a3a;--txt:#eee;--dim:#9a9aae;--acc:#7c6cff}\n*{box-sizing:border-box}\nbody{margin:0;background:var(--bg);color:var(--txt);font-family:system-ui,sans-serif}\n.wrap{max-width:820px;margin:0 auto;padding:26px 16px 70px}\nh1{letter-spacing:2px;margin:0 0 6px}\nh3{margin:22px 0 8px;font-size:15px;color:var(--acc);letter-spacing:1px;text-transform:uppercase}\n.sub{color:var(--dim);font-size:14px;line-height:1.55;margin:0 0 16px}\n.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px}\nlabel{font-size:13px;color:var(--dim);display:block;margin-bottom:6px}\n.row{display:flex;gap:8px;flex-wrap:wrap}\ninput[type=text]{flex:1;min-width:220px;padding:12px 14px;border-radius:10px;border:1px solid #333;background:#10101a;color:#fff;font-size:15px;outline:none}\ninput[type=text]:focus{border-color:var(--acc)}\nbutton{padding:11px 18px;border:0;border-radius:10px;background:var(--acc);color:#fff;font-size:15px;cursor:pointer}\nbutton.alt{background:#2b2b3d}\nbutton:disabled{opacity:.5;cursor:default}\n.chk{display:flex;align-items:center;gap:8px;margin-top:12px;font-size:14px;color:var(--txt)}\n.small{font-size:12px;color:var(--dim);margin-top:10px;line-height:1.5}\n.res{display:flex;gap:10px;align-items:flex-start;padding:9px 12px;border-bottom:1px solid var(--line);font-size:14px}\n.res:last-child{border-bottom:0}\n.ic{width:22px;flex:none;text-align:center}\n.nm{font-weight:600}\n.dt{color:var(--dim);white-space:pre-wrap;word-break:break-word;font-size:13px;margin-top:2px}\n.bar{display:flex;gap:10px;align-items:center;margin:18px 0 10px;flex-wrap:wrap}\n#status{color:#7ddc9a;font-size:14px}\ntextarea{width:100%;height:240px;background:#10101a;color:#ddd;border:1px solid #333;border-radius:10px;padding:12px;font-family:monospace;font-size:12.5px;display:none}\n.stage{background:#000;border-radius:12px;overflow:hidden;border:1px solid var(--line);min-height:40px}\n.frame{width:100%;height:300px;border:0;background:#fff;display:block}\n.sum{display:flex;gap:14px;font-size:14px;margin:14px 0 4px}\n.mini{display:flex;gap:6px;margin-top:6px;align-items:center;flex-wrap:wrap}\n.mini button{padding:4px 10px;font-size:12px;background:#262636;border-radius:8px}\n.mini button.on.yes{background:#1f9d55}.mini button.on.maybe{background:#c98a00}.mini button.on.no{background:#c0392b}\n.mini a{color:#9a8cff;font-size:12px;margin-right:6px;text-decoration:none}\n.note{color:var(--dim);font-size:12.5px;margin:0 0 8px;line-height:1.5}\n</style>\n</head>\n<body>\n<div class=\"wrap\">\n  <h1>PROXY TESTER</h1>\n  <p class=\"sub\">Checks what your browser, your website host, and your proxy can and can't do, then gives you one report to copy.\n  The browser checks work on their own. Enter your proxy's address to test the proxy too.</p>\n\n  <div class=\"card\">\n    <label for=\"proxy\">Your proxy's address</label>\n    <div class=\"row\">\n      <input type=\"text\" id=\"proxy\" placeholder=\"https://your-proxy.example.com\" autocomplete=\"off\" spellcheck=\"false\">\n      <button id=\"run\">&#9654; Run all tests</button>\n      <button id=\"stop\" class=\"alt\" disabled>&#9632; Stop</button>\n    </div>\n    <label class=\"chk\"><input type=\"checkbox\" id=\"sites\" checked> Also open real websites through the proxy (about 20 seconds each)</label>\n    <div class=\"small\">Using the proxy on your own computer? Enter <b>http://localhost:8080</b>.<br>\n    If this page is on https, your proxy address must be https too (localhost is the one exception).\n    Tip: add <b>?sites=https://a.com,https://b.com</b> to this page's address to test your own list.</div>\n  </div>\n\n  <div class=\"sum\" id=\"sum\"></div>\n  <div id=\"results\"></div>\n\n  <div class=\"bar\">\n    <button id=\"copy\">&#128203; Copy report</button>\n    <span id=\"status\"></span>\n  </div>\n  <textarea id=\"out\" readonly></textarea>\n\n  <h3>Live preview</h3>\n  <div class=\"stage\" id=\"stage\"></div>\n</div>\n\n<script>\nvar DEFAULT_SITES=[\n  {name:\"example.com\",url:\"https://example.com\"},\n  {name:\"YouTube\",url:\"https://www.youtube.com\"},\n  {name:\"poxel.io\",url:\"https://poxel.io\"},\n  {name:\"Wikipedia\",url:\"https://en.wikipedia.org/wiki/Special:Random\"},\n  {name:\"DuckDuckGo\",url:\"https://duckduckgo.com/html/?q=cats\"},\n  {name:\"Reddit\",url:\"https://www.reddit.com\"},\n  {name:\"GitHub\",url:\"https://github.com\"},\n  {name:\"Twitch\",url:\"https://www.twitch.tv\"},\n  {name:\"Discord\",url:\"https://discord.com\"},\n  {name:\"Krunker\",url:\"https://krunker.io\"},\n  {name:\"Shell Shockers\",url:\"https://shellshock.io\"},\n  {name:\"Slither.io\",url:\"https://slither.io\"},\n  {name:\"1v1.LOL\",url:\"https://1v1.lol\"},\n  {name:\"CrazyGames\",url:\"https://www.crazygames.com\"},\n  {name:\"Hacker News\",url:\"https://news.ycombinator.com\"},\n  {name:\"Stack Overflow\",url:\"https://stackoverflow.com\"}\n];\n\n// PURE-START\nfunction normalizeProxy(v){\n  v=(v||\"\").trim().replace(/\\/+$/,\"\");\n  if(!v)return \"\";\n  if(!/^https?:\\/\\//i.test(v))v=(/^(localhost|127\\.|192\\.168\\.|10\\.)/i.test(v)?\"http://\":\"https://\")+v;\n  return v;\n}\nfunction wsUrl(p){return p.replace(/^http/i,\"ws\");}\nfunction siteKey(u){\n  try{\n    var h=new URL(u).hostname.split(\".\");\n    if(/^\\d+(\\.\\d+){3}$/.test(h.join(\".\")))return h.join(\".\");\n    return h.length>2?h.slice(-2).join(\".\"):h.join(\".\");\n  }catch(e){return u;}\n}\nfunction topErrors(errs,n){\n  var c={},order=[];\n  (errs||[]).forEach(function(e){\n    var k=e.type+\" \"+e.msg;\n    if(!c[k]){c[k]=0;order.push(k);}\n    c[k]++;\n  });\n  order.sort(function(a,b){return c[b]-c[a];});\n  return order.slice(0,n).map(function(k){return (c[k]>1?c[k]+\"x \":\"\")+k;});\n}\nfunction rateSite(m){\n  var errs=m.errors||[];\n  var count=function(list){return errs.filter(function(e){return list.indexOf(e.type)>-1;}).length;};\n  var net=errs.filter(function(e){return e.type===\"fetch-failed\"||e.type===\"xhr-failed\"||e.type.indexOf(\"http-\")===0;}).length,\n      res=count([\"resource-failed\"]),\n      ws=Math.max(count([\"ws-error\"]),count([\"ws-closed\"])),\n      js=count([\"js-error\",\"promise-rejected\"]);\n  if(!m.started)return {status:\"fail\",summary:\"Nothing came back. The proxy never delivered the page (blocked, timed out, or the site refused).\"};\n  if(!m.loaded&&!m.domReady)return {status:\"fail\",summary:\"The page started but never finished loading.\"};\n  var blank=!!(m.snapshot&&m.snapshot.text<30&&m.snapshot.canvas===0&&m.snapshot.imgs===0);\n  var bits=[];\n  if(blank)bits.push(\"page looks blank\");\n  if(net)bits.push(net+\" failed request\"+(net>1?\"s\":\"\"));\n  if(ws)bits.push(ws+\" WebSocket problem\"+(ws>1?\"s\":\"\"));\n  if(js)bits.push(js+\" script error\"+(js>1?\"s\":\"\"));\n  if(res)bits.push(res+\" missing file\"+(res>1?\"s\":\"\"));\n  var bad=blank||net>=3||ws>0||res>=5;\n  if(!bits.length)return {status:\"pass\",summary:\"Loaded fine\"+(m.snapshot&&m.snapshot.title?\" (\\\"\"+m.snapshot.title.slice(0,50)+\"\\\")\":\"\")};\n  return {status:bad?\"warn\":\"pass\",summary:(bad?\"Loaded, but: \":\"Loaded (minor noise): \")+bits.join(\", \")};\n}\nfunction buildReport(meta,results){\n  var tag={pass:\"PASS\",warn:\"WARN\",fail:\"FAIL\",info:\"INFO\"};\n  var n={pass:0,warn:0,fail:0};\n  results.forEach(function(r){if(n[r.status]!==undefined)n[r.status]++;});\n  var t=\"PROXY TESTER REPORT (\"+meta.date+\")\\n\";\n  t+=\"Tester page: \"+meta.page+\"\\n\";\n  t+=\"Proxy: \"+(meta.proxy||\"(none entered)\")+\"\\n\";\n  t+=\"Browser: \"+meta.ua+\"\\n\";\n  t+=\"Summary: \"+n.pass+\" PASS, \"+n.warn+\" WARN, \"+n.fail+\" FAIL\\n\";\n  var last=null;\n  results.forEach(function(r){\n    if(r.section!==last){t+=\"\\n== \"+r.section+\" ==\\n\";last=r.section;}\n    var lines=String(r.detail||\"\").split(\"\\n\");\n    var you=r.manual?{yes:\"WORKS\",maybe:\"PARTLY\",no:\"BROKEN\"}[r.manual]:\"\";\n    t+=\"[\"+tag[r.status]+\"] \"+r.name+(lines[0]?\" - \"+lines[0]:\"\")+(you?\"   <== YOU SAID: \"+you:\"\")+\"\\n\";\n    for(var i=1;i<lines.length;i++)t+=\"       \"+lines[i]+\"\\n\";\n  });\n  return t;\n}\n// PURE-END\n\nvar $=function(id){return document.getElementById(id);};\nvar results=[],running=false,stopFlag=false;\nvar ICON={pass:\"\\u2705\",warn:\"\\u26A0\\uFE0F\",fail:\"\\u274C\",info:\"\\u2139\\uFE0F\"};\n\nfunction render(){\n  var host=$(\"results\");host.innerHTML=\"\";\n  var n={pass:0,warn:0,fail:0},last=null,card=null;\n  results.forEach(function(r){\n    if(n[r.status]!==undefined)n[r.status]++;\n    if(r.section!==last){\n      var h=document.createElement(\"h3\");h.textContent=r.section;host.appendChild(h);\n      if(r.site){var nt=document.createElement(\"p\");nt.className=\"note\";nt.textContent=\"These ratings are automatic guesses and can be wrong. Click Open to try a site yourself, then tell me what really happened with Worked / Partly / Broken before you copy the report.\";host.appendChild(nt);}\n      card=document.createElement(\"div\");card.className=\"card\";card.style.padding=\"4px 0\";host.appendChild(card);\n      last=r.section;\n    }\n    var row=document.createElement(\"div\");row.className=\"res\";\n    var ic=document.createElement(\"div\");ic.className=\"ic\";ic.textContent=ICON[r.status]||\"\";\n    var body=document.createElement(\"div\");\n    var nm=document.createElement(\"div\");nm.className=\"nm\";nm.textContent=r.name;body.appendChild(nm);\n    if(r.detail){var dt=document.createElement(\"div\");dt.className=\"dt\";dt.textContent=r.detail;body.appendChild(dt);}\n    if(r.site){\n      var mini=document.createElement(\"div\");mini.className=\"mini\";\n      if(r.openUrl){var oa=document.createElement(\"a\");oa.href=r.openUrl;oa.target=\"_blank\";oa.textContent=\"Open \\u2197\";mini.appendChild(oa);}\n      var lab=document.createElement(\"span\");lab.textContent=\"You say:\";lab.style.cssText=\"font-size:12px;color:#9a9aae\";mini.appendChild(lab);\n      [[\"yes\",\"Worked\"],[\"maybe\",\"Partly\"],[\"no\",\"Broken\"]].forEach(function(o){\n        var b=document.createElement(\"button\");b.className=o[0]+(r.manual===o[0]?\" on\":\"\");b.textContent=o[1];\n        b.onclick=function(){r.manual=(r.manual===o[0])?null:o[0];render();};\n        mini.appendChild(b);\n      });\n      body.appendChild(mini);\n    }\n    row.appendChild(ic);row.appendChild(body);card.appendChild(row);\n  });\n  $(\"sum\").textContent=results.length?(\"\\u2705 \"+n.pass+\"   \\u26A0\\uFE0F \"+n.warn+\"   \\u274C \"+n.fail):\"\";\n}\nfunction add(section,name,status,detail,extra){var r={section:section,name:name,status:status,detail:detail||\"\"};if(extra)for(var k in extra)r[k]=extra[k];results.push(r);render();}\nfunction say(t){$(\"status\").textContent=t;}\nfunction sleep(ms){return new Promise(function(r){setTimeout(r,ms);});}\nfunction fetchT(url,ms){\n  var c=new AbortController();var t=setTimeout(function(){c.abort();},ms);\n  return fetch(url,{cache:\"no-store\",signal:c.signal}).then(function(r){clearTimeout(t);return r;},function(e){clearTimeout(t);throw e;});\n}\nfunction wsEcho(url,ms){\n  return new Promise(function(resolve){\n    var t0=Date.now(),done=false,w;\n    var finish=function(o){if(done)return;done=true;try{w.close();}catch(e){}resolve(o);};\n    var msg=\"ping-\"+Math.random().toString(36).slice(2);\n    try{w=new WebSocket(url);}catch(e){return resolve({ok:false,err:String(e.message||e)});}\n    w.onopen=function(){w.send(msg);};\n    w.onmessage=function(e){finish(e.data===msg?{ok:true,ms:Date.now()-t0}:{ok:false,err:\"unexpected reply\"});};\n    w.onerror=function(){finish({ok:false,err:\"connection error\"});};\n    w.onclose=function(e){finish({ok:false,err:\"closed (code \"+e.code+\")\"});};\n    setTimeout(function(){finish({ok:false,err:\"timed out\"});},ms);\n  });\n}\n\n/* ---------- Part 1: this browser / website ---------- */\nasync function envChecks(){\n  var S=\"This browser and website\";\n  add(S,\"Page address\",\"info\",location.href);\n  var framed=false;try{framed=window.top!==window;}catch(e){framed=true;}\n  add(S,\"Running inside a frame\",framed?\"warn\":\"pass\",framed?\"Yes (iframe or about:blank) - some features can be limited\":\"No - normal page\");\n  add(S,\"Secure page (HTTPS)\",window.isSecureContext?\"pass\":\"warn\",window.isSecureContext?\"Yes\":\"No - service workers, clipboard and many features need HTTPS\");\n  try{\n    document.cookie=\"pt_test=1; path=/; SameSite=Lax\";\n    var ok=document.cookie.indexOf(\"pt_test=1\")>-1;\n    document.cookie=\"pt_test=; path=/; max-age=0\";\n    add(S,\"Cookies\",ok?\"pass\":\"warn\",ok?\"Work\":\"Blocked\");\n  }catch(e){add(S,\"Cookies\",\"warn\",\"Blocked: \"+e);}\n  try{localStorage.setItem(\"pt\",\"1\");localStorage.removeItem(\"pt\");add(S,\"Local storage\",\"pass\",\"Works\");}\n  catch(e){add(S,\"Local storage\",\"warn\",\"Blocked: \"+e);}\n  add(S,\"IndexedDB\",window.indexedDB?\"pass\":\"warn\",window.indexedDB?\"Available\":\"Missing\");\n  try{\n    var url=URL.createObjectURL(new Blob([\"onmessage=function(e){postMessage(e.data+1)}\"],{type:\"text/javascript\"}));\n    var w=new Worker(url);\n    var r=await new Promise(function(res){\n      var t=setTimeout(function(){res(null);},2500);\n      w.onmessage=function(e){clearTimeout(t);res(e.data);};\n      w.onerror=function(){clearTimeout(t);res(null);};\n      w.postMessage(1);\n    });\n    w.terminate();\n    add(S,\"Web Workers\",r===2?\"pass\":\"warn\",r===2?\"Work\":\"Did not respond\");\n  }catch(e){add(S,\"Web Workers\",\"warn\",\"Blocked: \"+e);}\n  add(S,\"Service Workers\",(\"serviceWorker\" in navigator)?\"pass\":\"warn\",(\"serviceWorker\" in navigator)?\"Available\":\"Not available here (needs HTTPS)\");\n  var wasm=false;try{wasm=typeof WebAssembly===\"object\"&&WebAssembly.validate(new Uint8Array([0,97,115,109,1,0,0,0]));}catch(e){}\n  add(S,\"WebAssembly (games need it)\",wasm?\"pass\":\"fail\",wasm?\"Works\":\"Missing\");\n  var gl1=null,gl2=null,rend=\"\";\n  try{\n    var c=document.createElement(\"canvas\");\n    gl2=c.getContext(\"webgl2\");\n    gl1=gl2||document.createElement(\"canvas\").getContext(\"webgl\");\n    if(gl1){var ext=gl1.getExtension(\"WEBGL_debug_renderer_info\");if(ext)rend=gl1.getParameter(ext.UNMASKED_RENDERER_WEBGL)||\"\";}\n  }catch(e){}\n  var soft=/swiftshader|llvmpipe|software/i.test(rend);\n  add(S,\"WebGL (3D games need it)\",gl1?(soft?\"warn\":\"pass\"):\"fail\",gl1?((gl2?\"WebGL 2\":\"WebGL 1\")+(rend?\" - \"+rend:\"\")+(soft?\" (software rendering: games will be slow)\":\"\")):\"Not available - 3D games will not run\");\n  var sab=typeof SharedArrayBuffer!==\"undefined\";\n  add(S,\"Shared memory (some games need it)\",sab&&window.crossOriginIsolated?\"pass\":\"info\",sab?(window.crossOriginIsolated?\"Available\":\"Present but page is not cross-origin isolated\"):\"Not available on this page\");\n  add(S,\"Audio\",(window.AudioContext||window.webkitAudioContext)?\"pass\":\"warn\",(window.AudioContext||window.webkitAudioContext)?\"Available\":\"Missing\");\n  add(S,\"Fullscreen and mouse lock\",(document.fullscreenEnabled&&(\"requestPointerLock\" in Element.prototype))?\"pass\":\"info\",(document.fullscreenEnabled?\"Fullscreen OK\":\"Fullscreen blocked\")+\", \"+((\"requestPointerLock\" in Element.prototype)?\"mouse lock OK\":\"mouse lock missing\"));\n  add(S,\"Clipboard\",(navigator.clipboard&&navigator.clipboard.writeText)?\"pass\":\"info\",(navigator.clipboard&&navigator.clipboard.writeText)?\"Available\":\"Not available (copy by hand)\");\n  try{\n    var rr=await fetchT(\"https://api.github.com/zen\",6000);\n    add(S,\"Internet from this browser\",rr.ok?\"pass\":\"warn\",\"HTTP \"+rr.status);\n  }catch(e){add(S,\"Internet from this browser\",\"warn\",\"Could not reach api.github.com (\"+e+\") - offline, or a blocker/filter is in the way\");}\n  var pub=await wsEcho(\"wss://echo.websocket.org\",5000);\n  if(!pub.ok)pub=await wsEcho(\"wss://ws.postman-echo.com/raw\",5000);\n  add(S,\"WebSocket to the public internet\",pub.ok?\"pass\":\"warn\",pub.ok?\"Works (\"+pub.ms+\" ms)\":\"Failed (\"+pub.err+\"). The public test servers may be down, or WebSockets are blocked on this network.\");\n}\n\n/* ---------- Part 2: the proxy ---------- */\nfunction probeSite(base,url,maxMs){\n  return new Promise(function(resolve){\n    var m={started:false,domReady:false,loaded:false,snapshot:null,errors:[]};\n    var fr=document.createElement(\"iframe\");fr.className=\"frame\";\n    $(\"stage\").innerHTML=\"\";$(\"stage\").appendChild(fr);\n    var snapAt=0,done=false,iv=null,t0=Date.now();\n    function finish(){\n      if(done)return;done=true;clearInterval(iv);\n      window.removeEventListener(\"message\",onMsg);\n      try{fr.src=\"about:blank\";}catch(e){}\n      resolve(m);\n    }\n    function onMsg(e){\n      if(e.source!==fr.contentWindow)return;\n      var d=e.data;if(!d||!d.__proxy)return;\n      if(d.__proxy===\"start\")m.started=true;\n      else if(d.__proxy===\"dom-ready\")m.domReady=true;\n      else if(d.__proxy===\"load\")m.loaded=true;\n      else if(d.__proxy===\"snapshot\"){m.snapshot={title:d.title,text:d.text,canvas:d.canvas,imgs:d.imgs};snapAt=Date.now();}\n      else if(d.__proxy===\"diag\")m.errors.push({type:d.type,msg:d.msg});\n    }\n    window.addEventListener(\"message\",onMsg);\n    iv=setInterval(function(){\n      if(stopFlag||Date.now()-t0>maxMs||(snapAt&&Date.now()-snapAt>2500))finish();\n    },250);\n    fr.src=base+\"/go?url=\"+encodeURIComponent(url)+\"&probe=1\";\n  });\n}\nasync function proxyChecks(base){\n  var S=\"Your proxy\";\n  var t0=performance.now(),port=null;\n  say(\"Checking the proxy...\");\n  try{\n    var r=await fetchT(base+\"/__ping\",8000);\n    await r.json();\n    add(S,\"Proxy is reachable\",\"pass\",Math.round(performance.now()-t0)+\" ms\");\n  }catch(e){\n    add(S,\"Proxy is reachable\",\"fail\",\"Could not reach \"+base+\" (\"+e+\").\\nCheck the address, that it is running (free hosts go to sleep - wait 30 seconds and try again), and use https:// if this page is https.\");\n    return false;\n  }\n  say(\"Running server checks...\");\n  try{\n    var r2=await fetchT(base+\"/__selftest\",30000);\n    var j=await r2.json();port=j.port;\n    j.checks.forEach(function(c){add(S,c.name,c.status,c.detail);});\n  }catch(e){add(S,\"Server self-test\",\"warn\",\"Could not run: \"+e);}\n  if(stopFlag)return true;\n  say(\"Testing WebSockets...\");\n  var d=await wsEcho(wsUrl(base)+\"/__echo\",7000);\n  add(S,\"WebSocket connection (games need this)\",d.ok?\"pass\":\"fail\",d.ok?\"Works (\"+d.ms+\" ms)\":\"Failed: \"+d.err+\". Your host may block WebSockets, which breaks multiplayer games.\");\n  if(port&&!stopFlag){\n    var rl=await wsEcho(wsUrl(base)+\"/ws?url=\"+encodeURIComponent(\"ws://127.0.0.1:\"+port+\"/__echo\"),8000);\n    add(S,\"WebSocket relay (proxying game servers)\",rl.ok?\"pass\":\"fail\",rl.ok?\"Works (\"+rl.ms+\" ms)\":\"Failed: \"+rl.err);\n  }\n  if(stopFlag)return true;\n  say(\"Loading a page through the proxy...\");\n  var pm=await probeSite(base,\"https://example.com\",15000);\n  var pr=rateSite(pm);\n  add(S,\"Proxied page loads (example.com)\",pr.status,pr.summary+(pr.status!==\"pass\"&&pm.errors.length?\"\\n\"+topErrors(pm.errors,4).join(\"\\n\"):\"\"));\n  return true;\n}\nasync function siteChecks(base){\n  var S=\"Websites through the proxy\";\n  var list=DEFAULT_SITES.slice(1);\n  var m=location.search.match(/[?&]sites=([^&]+)/);\n  if(m){\n    list=decodeURIComponent(m[1]).split(\",\").map(function(u){\n      u=u.trim();if(!/^https?:\\/\\//i.test(u))u=\"https://\"+u;\n      var h;try{var uu=new URL(u);h=uu.hostname+(uu.pathname.length>1?uu.pathname:\"\");}catch(e){h=u;}\n      return {name:h,url:u};\n    });\n  }\n  var sameOrigin=false;try{sameOrigin=new URL(base).origin===location.origin;}catch(e){}\n  for(var i=0;i<list.length;i++){\n    if(stopFlag)break;\n    var s=list[i];\n    say(\"Testing \"+s.name+\" (\"+(i+1)+\" of \"+list.length+\")...\");\n    var pm=await probeSite(base,s.url,22000);\n    var pr=rateSite(pm);\n    var lines=[];\n    if(pr.status!==\"pass\")lines=topErrors(pm.errors,6);\n    if(sameOrigin&&pr.status!==\"pass\"){\n      try{\n        var dr=await fetchT(base+\"/__diag?site=\"+encodeURIComponent(siteKey(s.url)),5000);\n        (await dr.json()).slice(0,8).forEach(function(l){lines.push(\"server: \"+l);});\n      }catch(e){}\n    }\n    add(S,s.name,pr.status,pr.summary+(lines.length?\"\\n\"+lines.join(\"\\n\"):\"\"),{site:true,openUrl:base+\"/go?url=\"+encodeURIComponent(s.url)});\n  }\n}\n\nasync function runAll(){\n  if(running)return;\n  running=true;stopFlag=false;results=[];render();\n  $(\"run\").disabled=true;$(\"stop\").disabled=false;$(\"out\").style.display=\"none\";\n  var base=normalizeProxy($(\"proxy\").value);\n  $(\"proxy\").value=base;\n  try{localStorage.setItem(\"ptProxy\",base);}catch(e){}\n  try{\n    await envChecks();\n    if(base){\n      var ok=await proxyChecks(base);\n      if(ok&&!stopFlag&&$(\"sites\").checked)await siteChecks(base);\n    }else add(\"Your proxy\",\"Proxy address\",\"info\",\"No proxy address entered, so only the browser checks were run.\");\n  }catch(e){add(\"Tester\",\"Unexpected error\",\"fail\",String(e&&e.stack||e));}\n  running=false;$(\"run\").disabled=false;$(\"stop\").disabled=true;\n  $(\"stage\").innerHTML=\"\";\n  say(stopFlag?\"Stopped.\":\"Done. Click Copy report.\");\n}\nasync function copyReport(){\n  var text=buildReport({date:new Date().toISOString().slice(0,10),page:location.href,proxy:normalizeProxy($(\"proxy\").value),ua:navigator.userAgent},results);\n  $(\"out\").value=text;$(\"out\").style.display=\"block\";\n  try{await navigator.clipboard.writeText(text);say(\"Copied!\");}\n  catch(e){$(\"out\").select();try{document.execCommand(\"copy\");say(\"Copied!\");}catch(x){say(\"Select the text below and copy it.\");}}\n}\n$(\"run\").onclick=runAll;\n$(\"stop\").onclick=function(){stopFlag=true;say(\"Stopping...\");};\n$(\"copy\").onclick=copyReport;\n(function init(){\n  var q=location.search.match(/[?&]proxy=([^&]+)/);\n  var saved=\"\";try{saved=localStorage.getItem(\"ptProxy\")||\"\";}catch(e){}\n  var def=\"\";\n  if(q)def=decodeURIComponent(q[1]);\n  else if(/\\/tester\\/?$/.test(location.pathname))def=location.origin;\n  else def=saved;\n  $(\"proxy\").value=def;\n  var servedByProxy=/\\/tester\\/?$/.test(location.pathname);\n  var auto=/[?&]auto=1/.test(location.search)||(servedByProxy&&!/[?&]auto=0/.test(location.search));\n  if(auto)setTimeout(runAll,400);\n})();\n</script>\n</body>\n</html>\n";
const MANUAL_TEST_HTML = "<!DOCTYPE html>\n<html>\n<head>\n<meta charset=\"utf-8\">\n<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n<title>Site Test</title>\n<style>\nbody{margin:0;background:#0e0e14;color:#eee;font-family:system-ui,sans-serif}\n.wrap{max-width:760px;margin:0 auto;padding:28px 16px 60px}\nh1{letter-spacing:2px;margin:0 0 6px}\n.sub{color:#999;font-size:14px;line-height:1.5;margin:0 0 18px}\n.bar{display:flex;gap:10px;align-items:center;margin-bottom:18px;flex-wrap:wrap}\n.bar a{color:#7c6cff;margin-left:auto}\nbutton{padding:10px 16px;border:0;border-radius:10px;background:#7c6cff;color:#fff;font-size:15px;cursor:pointer}\n.row{display:flex;align-items:center;gap:10px;background:#181822;border-radius:12px;padding:10px 14px;margin-bottom:8px;flex-wrap:wrap}\n.row .n{flex:1;min-width:150px;font-weight:600}\n.row a{color:#9a8cff;text-decoration:none;font-size:14px}\n.r{background:#262636;padding:8px 14px;font-size:14px}\n.r.yes.on{background:#1f9d55}\n.r.maybe.on{background:#c98a00}\n.r.no.on{background:#c0392b}\ntextarea{display:none;width:100%;box-sizing:border-box;height:220px;margin-top:16px;background:#181822;color:#ddd;border:1px solid #333;border-radius:10px;padding:12px;font-family:monospace;font-size:13px}\n#status{color:#7ddc9a;font-size:14px}\n</style>\n</head>\n<body>\n<div class=\"wrap\">\n  <h1>SITE TEST</h1>\n  <p class=\"sub\">Click <b>Open</b> on a site, see what happens, then rate it.<br>\n  <b>Yes</b> = fully works &nbsp;·&nbsp; <b>Maybe</b> = loads but something is wrong (missing parts, \"no internet\") &nbsp;·&nbsp; <b>No</b> = doesn't work</p>\n  <div class=\"bar\">\n    <button id=\"new\">🎲 New 10 sites</button>\n    <button id=\"copy\">📋 Copy results</button>\n    <span id=\"status\"></span>\n    <a href=\"/\">← Home</a>\n  </div>\n  <div id=\"list\"></div>\n  <textarea id=\"out\" readonly></textarea>\n</div>\n<script>\nvar SITES=[\n  {name:\"YouTube\",url:\"https://www.youtube.com\"},\n  {name:\"poxel.io\",url:\"https://poxel.io\"},\n  {name:\"Wikipedia\",url:\"https://en.wikipedia.org/wiki/Special:Random\"},\n  {name:\"DuckDuckGo search\",url:\"https://duckduckgo.com/html/?q=cats\"},\n  {name:\"Reddit\",url:\"https://www.reddit.com\"},\n  {name:\"GitHub\",url:\"https://github.com\"},\n  {name:\"Twitch\",url:\"https://www.twitch.tv\"},\n  {name:\"Discord\",url:\"https://discord.com\"},\n  {name:\"Google\",url:\"https://www.google.com\"},\n  {name:\"Bing\",url:\"https://www.bing.com\"},\n  {name:\"Krunker\",url:\"https://krunker.io\"},\n  {name:\"Shell Shockers\",url:\"https://shellshock.io\"},\n  {name:\"Slither.io\",url:\"https://slither.io\"},\n  {name:\"Agar.io\",url:\"https://agar.io\"},\n  {name:\"1v1.LOL\",url:\"https://1v1.lol\"},\n  {name:\"CrazyGames\",url:\"https://www.crazygames.com\"},\n  {name:\"Poki\",url:\"https://poki.com\"},\n  {name:\"example.com\",url:\"https://example.com\"},\n  {name:\"Hacker News\",url:\"https://news.ycombinator.com\"},\n  {name:\"BBC\",url:\"https://www.bbc.com\"},\n  {name:\"IMDb\",url:\"https://www.imdb.com\"},\n  {name:\"Stack Overflow\",url:\"https://stackoverflow.com\"},\n  {name:\"Internet Archive\",url:\"https://archive.org\"},\n  {name:\"Spotify\",url:\"https://open.spotify.com\"},\n  {name:\"Pinterest\",url:\"https://www.pinterest.com\"}\n];\n\n// PURE-START\nfunction siteKey(u){\n  try{\n    var h=new URL(u).hostname.split(\".\");\n    if(/^\\d+(\\.\\d+){3}$/.test(h.join(\".\")))return h.join(\".\");\n    return h.length>2?h.slice(-2).join(\".\"):h.join(\".\");\n  }catch(e){return u;}\n}\nfunction pickTen(list,rand){\n  var a=list.slice();\n  for(var i=a.length-1;i>0;i--){var j=Math.floor(rand()*(i+1));var t=a[i];a[i]=a[j];a[j]=t;}\n  return a.slice(0,10);\n}\nfunction buildReport(picks,ratings,debug,date){\n  var g={yes:[],maybe:[],no:[],none:[]};\n  picks.forEach(function(s){(g[ratings[s.url]]||g.none).push(s.name);});\n  var t=\"MY PROXY TEST RESULTS (\"+date+\")\\n\";\n  t+=\"YES (\"+g.yes.length+\"): \"+(g.yes.join(\", \")||\"-\")+\"\\n\";\n  t+=\"MAYBE (\"+g.maybe.length+\"): \"+(g.maybe.join(\", \")||\"-\")+\"\\n\";\n  t+=\"NO (\"+g.no.length+\"): \"+(g.no.join(\", \")||\"-\")+\"\\n\";\n  t+=\"NOT TESTED (\"+g.none.length+\"): \"+(g.none.join(\", \")||\"-\")+\"\\n\";\n  var dbg=picks.filter(function(s){return ratings[s.url]===\"maybe\"||ratings[s.url]===\"no\";});\n  if(dbg.length){\n    t+=\"\\nDEBUG INFO (maybe/no sites)\\n\";\n    dbg.forEach(function(s){\n      t+=\"[\"+s.name+\"] marked \"+ratings[s.url].toUpperCase()+\"\\n\";\n      var d=debug[s.url]||[];\n      t+=d.length?d.map(function(x){return \"  \"+x;}).join(\"\\n\")+\"\\n\":\"  (no errors recorded - did you click Open first?)\\n\";\n    });\n  }\n  return t;\n}\n// PURE-END\n\nvar picks=[],ratings={};\nvar $list=document.getElementById(\"list\"),$out=document.getElementById(\"out\"),\n    $copy=document.getElementById(\"copy\"),$status=document.getElementById(\"status\");\n\nfunction save(){try{localStorage.setItem(\"proxytest\",JSON.stringify({picks:picks,ratings:ratings}));}catch(e){}}\nfunction load(){\n  try{var d=JSON.parse(localStorage.getItem(\"proxytest\")||\"null\");if(d&&d.picks&&d.picks.length){picks=d.picks;ratings=d.ratings||{};return true;}}catch(e){}\n  return false;\n}\nfunction render(){\n  $list.innerHTML=\"\";\n  picks.forEach(function(s){\n    var row=document.createElement(\"div\");row.className=\"row\";\n    var n=document.createElement(\"span\");n.className=\"n\";n.textContent=s.name;\n    var a=document.createElement(\"a\");a.textContent=\"Open ↗\";a.target=\"_blank\";\n    a.href=\"/go?url=\"+encodeURIComponent(s.url);\n    row.appendChild(n);row.appendChild(a);\n    [\"yes\",\"maybe\",\"no\"].forEach(function(v){\n      var b=document.createElement(\"button\");\n      b.className=\"r \"+v+(ratings[s.url]===v?\" on\":\"\");\n      b.textContent=v.charAt(0).toUpperCase()+v.slice(1);\n      b.onclick=function(){\n        if(ratings[s.url]===v)delete ratings[s.url];else ratings[s.url]=v;\n        save();render();\n      };\n      row.appendChild(b);\n    });\n    $list.appendChild(row);\n  });\n}\ndocument.getElementById(\"new\").onclick=function(){\n  picks=pickTen(SITES,Math.random);ratings={};save();render();\n  $out.style.display=\"none\";$status.textContent=\"\";\n};\n$copy.onclick=async function(){\n  $copy.textContent=\"Collecting...\";\n  var debug={};\n  for(var i=0;i<picks.length;i++){\n    var s=picks[i];\n    if(ratings[s.url]===\"maybe\"||ratings[s.url]===\"no\"){\n      try{var r=await fetch(\"/__diag?site=\"+encodeURIComponent(siteKey(s.url)));debug[s.url]=await r.json();}\n      catch(e){debug[s.url]=[\"(could not read debug info)\"];}\n    }\n  }\n  var text=buildReport(picks,ratings,debug,new Date().toISOString().slice(0,10));\n  $out.value=text;$out.style.display=\"block\";\n  try{await navigator.clipboard.writeText(text);$status.textContent=\"Copied!\";}\n  catch(e){$out.select();try{document.execCommand(\"copy\");$status.textContent=\"Copied!\";}catch(x){$status.textContent=\"Select the text below and copy it\";}}\n  $copy.textContent=\"📋 Copy results\";\n};\nif(!load())picks=pickTen(SITES,Math.random);\nrender();\n</script>\n</body>\n</html>\n";

// The home page lives in home.html (next to this file) so it is easy to restyle.
// =====================================================================================
// SLOW LOG  (turn it on from the home page, or http://localhost:8080/slow)
// Records how long each request took and WHY (waiting for the real site, the proxy
// rewriting, downloading, searching for a lost file ...). Off by default = zero cost.
// =====================================================================================
let SLOW_ON = false;
const SLOW_MS = Number(process.env.SLOW_MS) || 1000;
const slowLog = [];
const hostStats = new Map();
function slowStart(req, res, t, t0) {
  if (!SLOW_ON) return null;
  const tm = { t0, host: t.hostname, path: (t.pathname + t.search).slice(0, 110), method: req.method };
  res.on("close", () => slowEnd(tm));
  return tm;
}
function slowMark(tm, k) {
  if (tm) tm[k] = Math.round(performance.now() - tm.t0);
}
function whySlow(e) {
  const check = e.check || 0;
  const up = e.upstream || check;
  const lost = e.lost || 0;
  const parts = [
    [lost, "searching other folders for a missing file"],
    [Math.max(0, up - check), "waiting for the real site to answer"],
    [e.processed ? Math.max(0, e.processed - up - lost) : 0, "the proxy rewriting the page/script"],
    [Math.max(0, e.total - (e.processed || up + lost)), "downloading (big file or slow connection)"],
    [check, "looking up / checking the address"],
  ];
  parts.sort((a, b) => b[0] - a[0]);
  return parts[0][1] + " (" + parts[0][0] + " ms)";
}
function slowEnd(tm) {
  tm.total = Math.round(performance.now() - tm.t0);
  const h = hostStats.get(tm.host) || { n: 0, sum: 0, max: 0, up: 0 };
  h.n++;
  h.sum += tm.total;
  h.up += tm.upstream || 0;
  if (tm.total > h.max) h.max = tm.total;
  hostStats.set(tm.host, h);
  if (hostStats.size > 500) hostStats.clear();
  if (tm.total >= SLOW_MS) {
    tm.when = Date.now();
    tm.why = whySlow(tm);
    slowLog.push(tm);
    if (slowLog.length > 200) slowLog.shift();
  }
}
function slowJson() {
  const worst = slowLog.reduce((a, b) => (!a || b.total > a.total ? b : a), null);
  return {
    on: SLOW_ON,
    ms: SLOW_MS,
    rewriteJs: REWRITE_JS,
    blockAds: BLOCK_ADS,
    slowCount: slowLog.length,
    worst: worst ? { host: worst.host, total: worst.total, why: worst.why } : null,
  };
}
function slowPage() {
  const onoff = (label, on, key) =>
    `<p>${label}: <b style="color:${on ? "#0f4" : "#f66"}">${on ? "ON" : "OFF"}</b> &nbsp; <a href="/slow?${key}=${on ? 0 : 1}">turn ${on ? "off" : "on"}</a></p>`;
  const rows = slowLog
    .slice()
    .reverse()
    .slice(0, 80)
    .map(
      (e) =>
        `<tr><td>${(e.total / 1000).toFixed(1)}s</td><td>${esc(e.host)}</td><td>${esc(e.path)}</td><td>${e.status || ""}</td><td>${esc(e.why || "")}</td></tr>`
    )
    .join("");
  const hosts = [...hostStats]
    .sort((a, b) => b[1].sum - a[1].sum)
    .slice(0, 15)
    .map(([h, s]) => `<tr><td>${esc(h)}</td><td>${s.n}</td><td>${Math.round(s.sum / s.n)} ms</td><td>${Math.round(s.up / s.n)} ms</td><td>${(s.max / 1000).toFixed(1)}s</td></tr>`)
    .join("");
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><title>mongle p - slow log</title>
${SLOW_ON ? '<meta http-equiv="refresh" content="6">' : ""}
<style>body{background:#000;color:#0f4;font-family:monospace;padding:24px;max-width:1100px;margin:auto}a{color:#7cff9c}
table{border-collapse:collapse;width:100%;margin:8px 0 22px}td,th{border-bottom:1px solid #144;padding:4px 8px;text-align:left;font-size:13px;word-break:break-all}
th{color:#9fe}h2{margin:22px 0 4px;color:#9fe}.d{color:#6a8}</style></head><body>
<h1>slow log</h1>
<p><a href="/">&larr; back to mongle p</a> &nbsp; <a href="/slow">refresh</a> &nbsp; <a href="/slow?clear=1">clear</a></p>
${onoff("slow log", SLOW_ON, "on")}
<p class="d">When it is ON, every request is timed. Anything slower than ${SLOW_MS / 1000}s is listed below with the reason. Load the slow page, then come back here and read (or copy) this page.</p>
<h2>quick switches (no restart needed)</h2>
${onoff("script location fix (helps Poxel-type games, costs some speed)", REWRITE_JS, "rewrite")}
${onoff("ad blocking (fast-fails ad hosts)", BLOCK_ADS, "ads")}
<h2>slowest requests</h2>
<table><tr><th>time</th><th>site</th><th>file</th><th>status</th><th>mostly because</th></tr>${rows || '<tr><td colspan="5" class="d">' + (SLOW_ON ? "nothing slow yet - load a page" : "turn the slow log ON, then load the slow page") + "</td></tr>"}</table>
<h2>by site (busiest first)</h2>
<table><tr><th>site</th><th>requests</th><th>avg total</th><th>avg wait for site</th><th>slowest</th></tr>${hosts}</table>
</body></html>`;
}

// =====================================================================================
// SEARCH  - a results page made by the proxy itself.
// Brave's own page is protected by Cloudflare CAPTCHAs that can never finish inside a proxy,
// so we ask for the results here on the server and show them on our own page.
//   1. Brave's official API (only if you set BRAVE_API_KEY)
//   2. Brave's results page, read by the proxy
//   3. DuckDuckGo's results page (safety net, with a note saying so)
// =====================================================================================
const BRAVE_HTML_URL = process.env.BRAVE_HTML_URL || "https://search.brave.com/search";
const BRAVE_API_URL = process.env.BRAVE_API_URL || "https://api.search.brave.com/res/v1/web/search";
const DDG_HTML_URL = process.env.DDG_HTML_URL || "https://html.duckduckgo.com/html/";
const SEARCH_CACHE = new Map();
const esc = (s) => String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
const stripTags = (s) => decodeEntities(String(s).replace(/<[^>]*>/g, " ")).replace(/\s+/g, " ").trim();

function braveSearchQuery(u) {
  if (u.hostname === "search.brave.com" && u.pathname === "/search" && !u.searchParams.has("direct")) {
    const q = u.searchParams.get("q");
    if (q && q.trim()) return q.trim();
  }
  return null;
}
function looksBlocked(status, body) {
  if (status === 403 || status === 429 || status === 503) return true;
  return body.length < 40000 && /challenge-platform|cf-chl|just a moment|verify you are human|are you a robot|captcha/i.test(body);
}
const isBraveHost = (h) => /(^|\.)brave\.(com|software)$/i.test(h);

function parseBraveHtml(html) {
  const out = [];
  const seen = new Set();
  const starts = [];
  const re = /<div\b[^>]*\bclass="(?:[^"]*\s)?snippet(?:\s[^"]*)?"[^>]*>/gi;
  let m;
  while ((m = re.exec(html))) starts.push(m.index);
  for (let i = 0; i < starts.length && out.length < 20; i++) {
    const block = html.slice(starts[i], Math.min(starts[i + 1] || Infinity, starts[i] + 7000));
    let url = null;
    let anchorInner = "";
    const are = /<a\b[^>]*\bhref="(https?:\/\/[^"]+)"[^>]*>([\s\S]*?)<\/a>/gi;
    let am;
    while ((am = are.exec(block))) {
      let h;
      try {
        h = new URL(decodeEntities(am[1]));
      } catch {
        continue;
      }
      if (isBraveHost(h.hostname)) continue;
      url = h.href;
      anchorInner = am[2];
      break;
    }
    if (!url || seen.has(url)) continue;
    const tm = /<(?:div|span|h\d)\b[^>]*\bclass="[^"]*\btitle\b[^"]*"[^>]*>([\s\S]*?)<\/(?:div|span|h\d)>/i.exec(block);
    const dm = /<(?:div|p|span)\b[^>]*\bclass="[^"]*(?:snippet-description|description|snippet-content)[^"]*"[^>]*>([\s\S]*?)<\/(?:div|p|span)>/i.exec(block);
    const title = stripTags(tm ? tm[1] : anchorInner);
    if (!title) continue;
    seen.add(url);
    out.push({ title: title.slice(0, 160), url, desc: dm ? stripTags(dm[1]).slice(0, 320) : "" });
  }
  return out;
}

function parseDdgHtml(html) {
  const out = [];
  const seen = new Set();
  const are = /<a\b([^>]*)>([\s\S]*?)<\/a>/gi;
  let m;
  while ((m = are.exec(html)) && out.length < 25) {
    const attrs = m[1];
    if (!/class="[^"]*\bresult__a\b/i.test(attrs)) continue;
    const hm = /href="([^"]*)"/i.exec(attrs);
    if (!hm) continue;
    let href = decodeEntities(hm[1]);
    if (href.startsWith("//")) href = "https:" + href;
    try {
      const u = new URL(href);
      if (u.searchParams.get("uddg")) href = u.searchParams.get("uddg");
    } catch {}
    if (!/^https?:\/\//i.test(href) || seen.has(href)) continue;
    const after = html.slice(are.lastIndex, are.lastIndex + 2500);
    const sm = /class="[^"]*\bresult__snippet\b[^"]*"[^>]*>([\s\S]*?)<\/(?:a|td|div)>/i.exec(after);
    seen.add(href);
    out.push({ title: stripTags(m[2]).slice(0, 160), url: href, desc: sm ? stripTags(sm[1]).slice(0, 320) : "" });
  }
  return out;
}

const SEARCH_UA = UA;
async function searchGet(url, extra) {
  const hostname = new URL(url).hostname;
  const headers = Object.assign(
    {
      "user-agent": SEARCH_UA,
      accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      "accept-language": "en-US,en;q=0.9",
    },
    extra || {}
  );
  const ck = cookieHeader(hostname);
  if (ck) headers.cookie = ck;
  const r = await fetchRetry(url, { method: "GET", headers, redirect: "follow", signal: AbortSignal.timeout(8000) });
  storeCookies(hostname, r.headers.getSetCookie ? r.headers.getSetCookie() : []);
  return r;
}
async function braveApi(q, p) {
  const key = process.env.BRAVE_API_KEY;
  if (!key) throw new Error("no BRAVE_API_KEY set (optional)");
  const u = new URL(BRAVE_API_URL);
  u.searchParams.set("q", q);
  u.searchParams.set("count", "20");
  if (p) u.searchParams.set("offset", String(p));
  const r = await fetchRetry(u.href, { method: "GET", headers: { accept: "application/json", "x-subscription-token": key, "user-agent": SEARCH_UA }, signal: AbortSignal.timeout(8000) });
  if (r.status !== 200) throw new Error("Brave API said HTTP " + r.status + (r.status === 401 || r.status === 403 ? " (check BRAVE_API_KEY)" : r.status === 429 ? " (monthly/second limit reached)" : ""));
  const j = await r.json();
  const list = (j.web && j.web.results) || [];
  return { engine: "Brave (official API)", results: list.map((x) => ({ title: stripTags(x.title || x.url), url: x.url, desc: stripTags(x.description || "") })).filter((x) => /^https?:/i.test(x.url)) };
}
async function braveHtml(q, p) {
  const u = new URL(BRAVE_HTML_URL);
  u.searchParams.set("q", q);
  u.searchParams.set("source", "web");
  if (p) u.searchParams.set("offset", String(p));
  const r = await searchGet(u.href, { referer: "https://search.brave.com/" });
  const body = await r.text();
  if (looksBlocked(r.status, body)) throw new Error("Brave showed a human-check (HTTP " + r.status + ")");
  const results = parseBraveHtml(body);
  if (!results.length) {
    logDiag("search", "brave-parse-failed", "HTTP " + r.status + ", " + body.length + " bytes. Start of page: " + stripTags(body).slice(0, 160), u.href);
    throw new Error("Brave's page came back but no results could be read from it");
  }
  return { engine: "Brave", results };
}
async function ddgHtml(q) {
  const u = new URL(DDG_HTML_URL);
  u.searchParams.set("q", q);
  const r = await searchGet(u.href, { referer: "https://html.duckduckgo.com/" });
  const body = await r.text();
  const results = parseDdgHtml(body);
  if (!results.length) throw new Error(looksBlocked(r.status, body) ? "DuckDuckGo showed a human-check" : "no results could be read");
  return { engine: "DuckDuckGo", results };
}
async function runSearch(q, p, force) {
  const key = q + "|" + p + "|" + (force || "");
  const hit = SEARCH_CACHE.get(key);
  if (hit && Date.now() - hit.t < 300000) return hit.data;
  const t0 = Date.now();
  const tried = [];
  const chain = force === "ddg" ? [["DuckDuckGo", ddgHtml]] : [["Brave API", braveApi], ["Brave", braveHtml], ["DuckDuckGo", ddgHtml]];
  let data = null;
  for (const [label, fn] of chain) {
    try {
      const r = await fn(q, p);
      if (r.results.length) {
        data = { engine: r.engine, results: r.results };
        break;
      }
      tried.push(label + ": nothing found");
    } catch (e) {
      if (!/no BRAVE_API_KEY/.test(String(e.message))) tried.push(label + ": " + (e.cause && e.cause.message ? e.cause.message : e.message));
    }
  }
  data = data || { engine: null, results: [] };
  data.tried = tried;
  data.ms = Date.now() - t0;
  if (tried.length) logDiag("search", "search-fallback", tried.join(" | "), "");
  if (data.results.length) {
    if (SEARCH_CACHE.size > 200) SEARCH_CACHE.clear();
    SEARCH_CACHE.set(key, { t: Date.now(), data });
  }
  return data;
}
function searchPage(q, p, force, data) {
  const go = (u) => "/go?url=" + encodeURIComponent(u);
  const enc = encodeURIComponent(q);
  const items = data.results
    .map((r) => {
      let host = "";
      try {
        host = new URL(r.url).hostname;
      } catch {}
      return `<div class="r"><a class="t" href="${esc(go(r.url))}">${esc(r.title)}</a><div class="u">${esc(host)}</div><div class="s">${esc(r.desc)}</div></div>`;
    })
    .join("");
  const note =
    data.engine && data.engine !== "Brave" && data.engine !== "Brave (official API)" && force !== "ddg"
      ? `<div class="n">Brave could not be used right now (${esc((data.tried || []).join("; ") || "unknown reason")}), so these results are from ${esc(data.engine)}.${process.env.BRAVE_API_KEY ? "" : " To make Brave always work, add a free Brave Search API key - see the README."}</div>`
      : "";
  const none = data.results.length
    ? ""
    : `<div class="n">No results could be loaded.<br>${esc((data.tried || []).join(" | "))}<br>Try one of the links above.</div>`;
  const next = data.results.length >= 10 && data.engine && data.engine.startsWith("Brave") ? `<p><a href="/search?q=${enc}&p=${p + 1}">next page &gt;</a></p>` : "";
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${esc(q)} - search</title>
<style>body{background:#000;color:#0f4;font-family:monospace;margin:0;padding:18px 22px;max-width:860px}a{color:#7cff9c}
form{display:flex;gap:8px;margin-bottom:10px}input{flex:1;background:#020;border:1px solid #0a3;color:#0f4;padding:10px;font:inherit}
button{background:#031;color:#0f4;border:1px solid #0a3;padding:10px 16px;font:inherit;cursor:pointer}
.tabs{font-size:13px;margin:0 0 16px;color:#6a8}.tabs a{margin-right:14px}.r{margin:0 0 20px}.t{font-size:17px;text-decoration:none;color:#9fe}.t:hover{text-decoration:underline}
.u{color:#4a7;font-size:12px;margin:2px 0}.s{color:#aca;font-size:14px;line-height:1.45}.n{border:1px solid #0a3;padding:10px;margin:0 0 18px;color:#fc6;font-size:13px;line-height:1.5}.m{color:#365;font-size:12px}</style></head><body>
<form action="/search"><input name="q" value="${esc(q)}" autocomplete="off" autofocus><button>search</button></form>
<div class="tabs"><a href="/search?q=${enc}">Brave</a><a href="/search?q=${enc}&e=ddg">DuckDuckGo</a><a href="${esc(go("https://www.google.com/search?q=" + enc))}">Google</a><a href="${esc(go("https://www.bing.com/search?q=" + enc))}">Bing</a><a href="${esc(go("https://search.brave.com/search?q=" + enc + "&direct=1"))}">Brave (real site)</a></div>
${note}${none}${items}${next}
<div class="m">${data.engine ? esc(data.engine) + " - " : ""}${data.results.length} results in ${data.ms} ms</div></body></html>`;
}

// A friendly page instead of Cloudflare's "prove you are human" page, which cannot work in a proxy
function challengePage(t) {
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><title>${esc(t.hostname)} wants a human check</title></head>
<body style="background:#000;color:#0f4;font-family:monospace;padding:30px;max-width:720px;line-height:1.5">
<h2>${esc(t.hostname)} is asking for a "prove you are human" check</h2>
<p>This site uses Cloudflare's bot check. That check looks at your real browser, and it cannot finish when the page runs through a proxy, so the site will not let this request in.</p>
<p>What you can try:</p><ul>
<li><a href="${esc("/go?url=" + encodeURIComponent(t.href))}" style="color:#7cff9c">try again</a> (it sometimes lets a retry through)</li>
<li>try a different site that has the same game</li>
<li>turn the "script location fix" and "ad blocking" off at <a href="/slow" style="color:#7cff9c">/slow</a> and retry</li></ul>
<p style="color:#6a8">(This was recorded in /debug as "cloudflare-challenge".)</p></body></html>`;
}

// Looks in other folders for a missing script, 6 at a time (it used to be one after another).
const missCache = new Map(); // site|file -> time until which we don't look again
async function probeFolders(cands, init, headers, realRef, parentSignal) {
  const started = Date.now();
  const ctrls = [];
  let i = 0;
  let hit = null;
  const worker = async () => {
    while (!hit && i < cands.length && Date.now() - started < 2500) {
      const cand = cands[i++];
      const c = new AbortController();
      ctrls.push({ c, cand });
      const timer = setTimeout(() => c.abort(), 2000);
      try {
        const r2 = await fetchRetry(cand.href, { ...init, signal: AbortSignal.any([parentSignal, c.signal]), headers: { ...headers, referer: realRef ? realRef.href : cand.origin + "/" } });
        clearTimeout(timer);
        if (r2.status >= 200 && r2.status < 300 && !hit) hit = { cand, r2 };
        else if (r2.body) r2.body.cancel().catch(() => {});
      } catch {
        clearTimeout(timer);
      }
    }
  };
  await Promise.all(Array.from({ length: 6 }, worker));
  for (const x of ctrls) if (!hit || x.cand !== hit.cand) x.c.abort();
  return hit;
}


// =====================================================================================
// PASSWORD MANAGER (save/autofill ONLY - never logs what you type)
// Entries are saved only when you click "Save this password" on the home page's panel.
// Values are encrypted at rest with a key you set once (PW_KEY env var, or a random key
// printed at startup) - not plaintext in the save file.
// =====================================================================================
const PW_FILE = path.join(__dirname, "mongle-passwords.json");
const PW_KEY = crypto.createHash("sha256").update(process.env.PW_KEY || (() => {
  const k = crypto.randomBytes(24).toString("base64url");
  console.log(`${NAME}: no PW_KEY set, so a random one was generated for this run.`);
  console.log(`${NAME}: set PW_KEY=${k} to keep using the same saved passwords next time.`);
  return k;
})()).digest();
function pwEncrypt(plain) {
  const iv = crypto.randomBytes(12);
  const c = crypto.createCipheriv("aes-256-gcm", PW_KEY, iv);
  const enc = Buffer.concat([c.update(plain, "utf8"), c.final()]);
  return Buffer.concat([iv, c.getAuthTag(), enc]).toString("base64");
}
function pwDecrypt(blob) {
  const buf = Buffer.from(blob, "base64");
  const iv = buf.subarray(0, 12), tag = buf.subarray(12, 28), enc = buf.subarray(28);
  const d = crypto.createDecipheriv("aes-256-gcm", PW_KEY, iv);
  d.setAuthTag(tag);
  return Buffer.concat([d.update(enc), d.final()]).toString("utf8");
}
function pwLoad() {
  try {
    return JSON.parse(fs.readFileSync(PW_FILE, "utf8"));
  } catch {
    return [];
  }
}
function pwSave(list) {
  fs.writeFileSync(PW_FILE, JSON.stringify(list));
}
function pwList() {
  return pwLoad().map((e) => ({ id: e.id, site: e.site, username: e.username }));
}
// A proxied page only gets entries for ITS OWN site, and never the password itself -
// the proxy fills the password field directly; the page's JavaScript never sees it.
function pwEntriesFor(site) {
  return pwLoad()
    .filter((e) => e.site === site)
    .map((e) => ({ id: e.id, username: e.username }));
}
async function handlePw(req, res, url) {
  res.setHeader("Content-Type", "application/json");
  if (req.method === "GET" && url.pathname === "/__pw/list") {
    return res.end(JSON.stringify(pwList()));
  }
  if (req.method === "GET" && url.pathname === "/__pw/for") {
    const site = siteKey(url.searchParams.get("site") || "");
    return res.end(JSON.stringify(pwEntriesFor(site)));
  }
  if (req.method === "POST" && url.pathname === "/__pw/save") {
    let d;
    try {
      d = JSON.parse((await readBody(req)).toString() || "{}");
    } catch {
      res.statusCode = 400;
      return res.end(JSON.stringify({ error: "bad request" }));
    }
    const site = siteKey(String(d.site || ""));
    const username = String(d.username || "").slice(0, 200);
    const password = String(d.password || "");
    if (!site || !username || !password) {
      res.statusCode = 400;
      return res.end(JSON.stringify({ error: "site, username and password are all required" }));
    }
    const list = pwLoad();
    const existing = list.find((e) => e.site === site && e.username === username);
    const enc = pwEncrypt(password);
    if (existing) existing.enc = enc;
    else list.push({ id: crypto.randomUUID(), site, username, enc });
    pwSave(list);
    return res.end(JSON.stringify({ ok: true }));
  }
  if (req.method === "POST" && url.pathname === "/__pw/fill") {
    let d;
    try {
      d = JSON.parse((await readBody(req)).toString() || "{}");
    } catch {
      res.statusCode = 400;
      return res.end(JSON.stringify({ error: "bad request" }));
    }
    const entry = pwLoad().find((e) => e.id === d.id);
    if (!entry) {
      res.statusCode = 404;
      return res.end(JSON.stringify({ error: "not found" }));
    }
    try {
      return res.end(JSON.stringify({ username: entry.username, password: pwDecrypt(entry.enc) }));
    } catch {
      res.statusCode = 500;
      return res.end(JSON.stringify({ error: "could not decrypt (wrong PW_KEY?)" }));
    }
  }
  if (req.method === "POST" && url.pathname === "/__pw/delete") {
    let d;
    try {
      d = JSON.parse((await readBody(req)).toString() || "{}");
    } catch {
      d = {};
    }
    pwSave(pwLoad().filter((e) => e.id !== d.id));
    return res.end(JSON.stringify({ ok: true }));
  }
  res.statusCode = 404;
  res.end(JSON.stringify({ error: "not found" }));
}


// =====================================================================================
// PER-SITE FIX LIST  (site-fixes.json) - "this site needs X turned off", not a global switch.
// =====================================================================================
const SITEFIX_FILE = path.join(__dirname, "site-fixes.json");
function siteFixLoad() {
  try { return JSON.parse(fs.readFileSync(SITEFIX_FILE, "utf8")); } catch { return {}; }
}
function siteFixSave(obj) { try { fs.writeFileSync(SITEFIX_FILE, JSON.stringify(obj, null, 1)); } catch {} }
function siteFixGet(site) { return siteFixLoad()[site] || {}; }
function siteFixSet(site, patch) {
  const all = siteFixLoad();
  all[site] = Object.assign({}, all[site], patch);
  siteFixSave(all);
  return all[site];
}

// =====================================================================================
// AUTO-RETRY: the CLIENT reports when a page looks blank (see CLIENT script). If this is
// the first time for that site, we turn off the two speedups most likely to break a page
// (script-location rewriting, ad blocking) for that site ONLY, and remember it.
// =====================================================================================
function autoRetryFix(site) {
  const cur = siteFixGet(site);
  if (cur.rewriteJs === false && cur.blockAds === false) return null; // already tried everything we have
  const patch = cur.rewriteJs !== false ? { rewriteJs: false } : { blockAds: false };
  logDiag(site, "info", "auto-retry: turning OFF " + Object.keys(patch)[0] + " for this site only", "");
  return siteFixSet(site, patch);
}

// =====================================================================================
// DIAGNOSIS: turn /debug's raw log for one site into one plain-language likely cause.
// =====================================================================================
function diagnose(site) {
  const lines = diagFor2(site);
  const has = (re) => lines.some((l) => re.test(l));
  const count = (re) => lines.filter((l) => re.test(l)).reduce((n, l) => n + (parseInt(l) || 1), 0);
  if (!lines.length) return "No problems recorded for this site yet. Load the page, wait ~10 seconds, then try again.";
  if (has(/cloudflare-challenge/)) return "This site is protected by a Cloudflare \"prove you are human\" check, which cannot pass through a proxy. There is no fix for this from here - try a different site.";
  if (has(/ws-error|ws-closed/) && !has(/^(?!.*ws-).*(pass|loaded fine)/i))
    return "The game's server is refusing the connection (WebSocket). This is usually the game server itself blocking proxied connections, or your network blocking WebSockets.";
  if (has(/ad-blocked/)) return "The ad-blocking list is blocking a file this site actually needs. Try: /slow?ads=0 (site-wide) - a proper per-site override is coming.";
  if (count(/http-403/) >= 3) return "The real site is refusing several requests with 403 Forbidden. This is often anti-bot protection noticing the proxy.";
  if (count(/http-404/) >= 3 && has(/info.*no known script folder/))
    return "This site splits its code into many small files, and some couldn't be found at their expected address. The code-splitting fix tried but didn't find them all.";
  if (has(/fetch-error.*ENOTFOUND|fetch-error.*EAI_AGAIN/)) return "This server's computer could not find the site's address (DNS). The site may be blocked on this network.";
  if (has(/fetch-error.*ECONNREFUSED|fetch-error.*ETIMEDOUT/)) return "The real site refused or didn't answer the connection. It may be down, or blocking this server.";
  if (has(/promise-rejected|js-error/) && count(/promise-rejected|js-error/) >= 4)
    return "The page's own JavaScript is throwing several errors, usually because it expected to be running on its real address. The script-location fix (REWRITE_JS) may need to stay on for this site, or there's a part of the page it still doesn't handle.";
  return "No single obvious cause - open /debug and read the raw list for the details.";
}
function diagFor2(site) {
  const counts = new Map();
  for (const d of diag) {
    if (d.site !== site) continue;
    let where = d.url;
    try { const u = new URL(d.url); where = u.origin + u.pathname; } catch {}
    const line = (d.type + " " + where + " " + d.msg).trim();
    counts.set(line, (counts.get(line) || 0) + 1);
  }
  return [...counts].sort((a, b) => b[1] - a[1]).map(([line, n]) => (n > 1 ? n + "x " + line : line));
}

// =====================================================================================
// REQUEST INSPECTOR: a live, always-on (cheap) log of every proxied request, for learning
// and debugging. http://localhost:8080/inspector
// =====================================================================================
const inspectorLog = [];
function inspectorNote(entry) {
  inspectorLog.push(entry);
  if (inspectorLog.length > 300) inspectorLog.shift();
}
function inspectorPage() {
  const rows = inspectorLog
    .slice()
    .reverse()
    .map(
      (e) =>
        `<tr><td>${new Date(e.t).toLocaleTimeString()}</td><td>${e.method}</td><td>${esc(e.host)}</td><td title="${esc(e.url)}">${esc(e.path)}</td><td>${e.status}</td><td>${e.ms}ms</td><td>${e.cache ? "cache" : ""}${e.rewritten ? " rewritten" : ""}${e.enc ? " " + e.enc : ""}</td></tr>`
    )
    .join("");
  return `<!DOCTYPE html><html><head><meta charset="utf-8"><title>mongle p - request inspector</title>
<meta http-equiv="refresh" content="3">
<style>body{background:#000;color:#0f4;font-family:monospace;padding:20px;max-width:1150px;margin:auto}
table{border-collapse:collapse;width:100%}td,th{border-bottom:1px solid #144;padding:3px 7px;text-align:left;font-size:12.5px;word-break:break-all}
th{color:#9fe}a{color:#7cff9c}</style></head><body>
<h1>request inspector</h1>
<p><a href="/">&larr; back</a> &nbsp; <a href="/inspector">refresh</a> &nbsp; <a href="/inspector?clear=1">clear</a></p>
<p style="color:#6a8;font-size:13px">Every request this proxy handles, newest first. "rewritten" = the proxy changed the file (script-location fix, or location.hostname etc.); "cache" = served from the proxy's own memory instead of asking the real site again.</p>
<table><tr><th>time</th><th>method</th><th>site</th><th>path</th><th>status</th><th>time</th><th>notes</th></tr>${rows || '<tr><td colspan="7" style="color:#6a8">nothing yet - load a page through the proxy</td></tr>'}</table>
</body></html>`;
}


let HOME;
try {
  HOME = fs.readFileSync(path.join(__dirname, "home.html"), "utf8");
} catch {
  HOME = `<!DOCTYPE html><html><head><meta charset="utf-8"><title>mongle p</title></head><body style="background:#000;color:#0f4;font-family:monospace;padding:30px"><h1>MONGLE_P</h1><form action="/go"><input name="url" placeholder="Search or enter a website" autofocus style="width:60%"> <button>GO</button></form><p><a style="color:#0f4" href="/tester">tester</a></p></body></html>`;
}

const server = http
  .createServer(async (req, res) => {
    const url = new URL(req.url, `http://localhost:${PORT}`);
    try {
      // A proxied page asked for something on OUR address (like location.href="/login").
      // Send it to the same real site instead.
      if (url.pathname === "/inspector") {
        if (url.searchParams.get("clear") === "1") inspectorLog.length = 0;
        if (url.searchParams.get("json") === "1") {
          res.setHeader("Content-Type", "application/json");
          return res.end(JSON.stringify(inspectorLog.slice(-50)));
        }
        res.setHeader("Content-Type", "text/html; charset=utf-8");
        return res.end(inspectorPage());
      }
      if (url.pathname === "/__autoretry" && req.method === "POST") {
        try {
          const d = JSON.parse((await readBody(req)).toString() || "{}");
          const fix = autoRetryFix(siteKey(d.site || ""));
          res.setHeader("Content-Type", "application/json");
          return res.end(JSON.stringify({ fix }));
        } catch {
          res.statusCode = 400;
          return res.end("{}");
        }
      }
      if (url.pathname === "/debug" && url.searchParams.get("diagnose") === "1") {
        res.setHeader("Content-Type", "text/plain; charset=utf-8");
        return res.end(diagnose(siteKey(url.searchParams.get("site") || "")));
      }
      if (url.pathname === "/__diag") {
        if (req.method === "POST") {
          try {
            const d = JSON.parse((await readBody(req)).toString() || "{}");
            logDiag(d.page || "", d.type || "client", d.msg || "", "");
          } catch {}
          res.statusCode = 204;
          return res.end();
        }
        res.setHeader("Content-Type", "application/json");
        return res.end(JSON.stringify(diagFor(siteKey(url.searchParams.get("site") || ""))));
      }
      if (url.pathname === "/__cookie" && req.method === "POST") {
        try {
          const d = JSON.parse((await readBody(req)).toString() || "{}");
          storeCookies(new URL(d.page).hostname, [String(d.cookie || "")]);
        } catch {}
        res.statusCode = 204;
        return res.end();
      }
      if (url.pathname === "/debug") {
        if (url.searchParams.get("clear") === "1") diag.length = 0;
        const sites = [...new Set(diag.map((d) => d.site))];
        let txt = NAME.toUpperCase() + " DEBUG  node " + process.version + "  " + process.platform + "\n";
        txt += "TLS-fallback hosts: " + ([...tlsFallbackHosts.keys()].join(", ") || "none") + "\n";
        txt += "Open /debug?clear=1 to clear this list, then try a game again.\n";
        for (const s of sites) {
          const counts = new Map();
          for (const d of diag) {
            if (d.site !== s) continue;
            let where = d.url;
            try {
              const u = new URL(d.url);
              where = u.origin + u.pathname;
            } catch {}
            const line = (d.type + " " + where + " " + d.msg).trim();
            counts.set(line, (counts.get(line) || 0) + 1);
          }
          txt += "\n== " + s + " ==\n";
          for (const [line, n] of [...counts].sort((a, b) => b[1] - a[1]).slice(0, 40)) txt += (n > 1 ? n + "x " : "") + line + "\n";
        }
        for (const [s, list] of scriptDirs) txt += "\n== script folders seen for " + s + " ==\n" + list.slice(0, 15).join("\n") + "\n";
        if (!sites.length) txt += "\n(nothing recorded yet - open a game through the proxy first)\n";
        res.writeHead(200, { "Content-Type": "text/plain; charset=utf-8", "Cache-Control": "no-store" });
        return res.end(txt);
      }
      if (url.pathname === "/clear") {
        // Wipes the browser's cached copies of proxied files (but NOT cookies/saved game data)
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Clear-Site-Data": '"cache"', "Cache-Control": "no-store" });
        return res.end('<body style="font-family:system-ui;background:#0e0e14;color:#eee;padding:30px">Cache cleared. <a style="color:#7c6cff" href="/">Back to the proxy</a></body>');
      }
      if (url.pathname.startsWith("/__pw/")) return await handlePw(req, res, url);
      if (url.pathname === "/search") {
        const q = (url.searchParams.get("q") || "").trim();
        if (!q) {
          res.writeHead(302, { Location: "/" });
          return res.end();
        }
        const p = Math.max(0, Number(url.searchParams.get("p")) || 0);
        const force = url.searchParams.get("e") === "ddg" ? "ddg" : null;
        const data = await runSearch(q, p, force);
        res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
        return res.end(searchPage(q, p, force, data));
      }
      if (url.pathname === "/slow") {
        if (url.searchParams.has("on")) SLOW_ON = url.searchParams.get("on") === "1";
        if (url.searchParams.has("rewrite")) REWRITE_JS = url.searchParams.get("rewrite") === "1";
        if (url.searchParams.has("ads")) BLOCK_ADS = url.searchParams.get("ads") === "1";
        if (url.searchParams.get("clear") === "1") {
          slowLog.length = 0;
          hostStats.clear();
        }
        if (url.searchParams.get("json") === "1") {
          res.setHeader("Content-Type", "application/json");
          return res.end(JSON.stringify(slowJson()));
        }
        res.setHeader("Content-Type", "text/html; charset=utf-8");
        return res.end(slowPage());
      }
      if (url.pathname === "/__ping") {
        res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store" });
        return res.end(JSON.stringify({ ok: true, t: Date.now() }));
      }
      if (url.pathname === "/__selftest") {
        const data = await selfTest();
        res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Cache-Control": "no-store" });
        return res.end(JSON.stringify(data));
      }
      if (url.pathname === "/tester") {
        res.setHeader("Content-Type", "text/html; charset=utf-8");
        return res.end(TESTER_HTML);
      }
      if (url.pathname === "/test") {
        res.setHeader("Content-Type", "text/html; charset=utf-8");
        return res.end(MANUAL_TEST_HTML);
      }
      const lost = lostRequestTarget(req);
      if (lost && url.pathname !== "/go") {
        res.writeHead(307, { Location: prox(lost) + "&lost=1" });
        return res.end();
      }
      if (url.pathname === "/") {
        res.setHeader("Content-Type", "text/html; charset=utf-8");
        return res.end(HOME);
      }
      if (url.pathname === "/__errors") {
        if (url.searchParams.get("clear") === "1") ERR_LOG.length = 0;
        if (url.searchParams.get("json") === "1") {
          res.setHeader("Content-Type", "application/json");
          return res.end(errorsPage(true));
        }
        res.setHeader("Content-Type", "text/html; charset=utf-8");
        return res.end(errorsPage(false));
      }
      if (url.pathname === "/go") return await handleGo(req, res, url);
      res.statusCode = 404;
      res.end("Not found");
    } catch (e) {
      noteError(`request ${req.method} ${req.url}`, e);
      if (!res.headersSent) res.statusCode = 500;
      try {
        res.end("Proxy error: " + e.message + "  (full details: /__errors)");
      } catch {}
    }
  })
  ;
server.on("error", (e) => {
  noteError("server", e);
  if (e && e.code === "EADDRINUSE") console.error(`[${NAME}] port ${PORT} is already in use - stop whatever else is using it, or set PORT to a different number.`);
});
server.keepAliveTimeout = 65000; // keep browser connections open between requests (fewer reconnects)
server.headersTimeout = 66000;
server.on("upgrade", handleUpgrade);
server.listen(PORT, () => console.log(`${NAME} running at http://localhost:${PORT}  (debug info: http://localhost:${PORT}/debug)`));
