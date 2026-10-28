MONGLE P

START:
  Double-click start.bat   (or open a terminal here and run: node proxy.js)
  Then open http://localhost:8080 in your browser.
  Press Ctrl+C in the window to stop it.

DEPLOYING AS A REAL WEBSITE (so anyone with the address can use it)
  mongle p is a real server (proxy.js), not a static page, so it has to be hosted
  somewhere that actually RUNS Node.js continuously - not GitHub Pages or any plain
  static-file host, which only serve files and can't run a program.

  Three files are already set up for this: package.json (tells any host how to start
  it), Procfile (the format Railway/Heroku-style hosts expect), and render.yaml (lets
  Render set itself up automatically from this one file).

  EASIEST OPTION - Render.com (has a free tier):
    1. Put this folder in a GitHub repository (or upload it directly if Render offers
       that).
    2. On Render: New -> Web Service -> connect the repo. It will read render.yaml
       and set everything up by itself - just click Create.
    3. Render gives you a real address like https://mongle-p-xxxx.onrender.com -
       that's your live proxy.

  ALSO WORKS - Railway.app, Replit, Glitch, Fly.io, or any host that runs Node.js:
    Upload the folder, make sure it runs "node proxy.js" (or "npm start" - same
    thing), and set PORT if the host doesn't set it for you automatically (most do).

  IMPORTANT ONCE IT'S LIVE
    - Set PW_KEY to a password you choose, in the host's "environment variables"
      settings - otherwise a new random one is generated every time the server
      restarts, and any saved logins from before become unreadable.
    - Free hosts often use a TEMPORARY disk: mongle-passwords.json and
      site-fixes.json can be wiped on every restart/redeploy unless the host offers
      a "persistent disk/volume" (Render does, on paid plans; check before relying
      on saved logins staying put).
    - Free hosts often "sleep" after inactivity - the first visit after sleeping can
      take 20-50 seconds to wake back up. This is normal, not a bug.
    - Anyone who knows the address can use your proxy once it's public - that also
      means anyone could browse THROUGH it. A password-protect-the-proxy feature is
      on the planned list (see the earlier feature list) but not built yet.
    - ALLOW_PRIVATE should stay OFF (its default) on a public deployment - it exists
      only for testing on your own machine, and turning it on publicly would let
      any visitor use your server to probe your host's own internal network.

SEARCH FREEZE - FIXED
  Typing a plain word into the search bar used to freeze the entire proxy. The cause:
  the search bar built the address '/search?q=...' (no 'http://', no dot in it), handed
  it back to the same "open this" function, which saw no scheme and no dot and decided
  it must be ANOTHER search term - so it searched for it again, which built another
  address and searched again, forever. That is now fixed in two places: the address is
  built as a full http://... address, and the proxy's own pages (search, tester, debug,
  slow, inspector) are opened directly instead of being wrapped and sent through the
  website-fetching pipeline (which correctly refuses to fetch the proxy's own address -
  that refusal is what showed as "Bad URL" once the first part of the fix was in place).

NEW IN THIS UPDATE
  - SEARCH: the search bar and the home page's "Brave" option now use the proxy's OWN
    results page at /search. Brave's real site uses a Cloudflare human-check that can
    never finish inside a proxy, so /search fetches Brave's results on the server; if
    that's blocked, it automatically falls back to DuckDuckGo and says so on the page.
    (Optional: set BRAVE_API_KEY for Brave's official API - faster and more reliable.
    See https://brave.com/search/api/ to get one; it has a small free monthly credit.)
  - PASSWORD MANAGER (save/autofill only - never logs what you type):
      * After you submit a login form, mongle p asks "Save this password for <site>?"
        at the bottom of the page. Only saves if you click Save.
      * On a site with a saved login, a small "fill saved login" button appears next
        to the password box - click it to fill both fields.
      * Saved passwords are encrypted on disk (mongle-passwords.json). Set PW_KEY to a
        password of your choice to keep using the same saved logins across restarts -
        otherwise a random key is generated each time you start mongle p (and printed
        once in the terminal), which means saved logins from a PREVIOUS run won't open.
      * The home page has a "saved logins" panel to remove any entry.
  - PER-SITE FIX LIST (site-fixes.json): instead of one global on/off switch, a site that
    needs the script-location fix or ad-blocking turned off gets it turned off for THAT
    SITE ONLY. Everything else keeps the speedups.
  - AUTO-RETRY: if a page loads blank, mongle p notices, turns off the script-location fix
    for that site, and reloads once automatically. If it's still blank, the next attempt
    also turns off ad-blocking. This is saved in site-fixes.json, so it only has to happen
    once per site.
  - DIAGNOSE BUTTON: a new toolbar button on a site tab reads /debug for that site and
    gives you one plain-English likely cause instead of a raw log.
  - REQUEST INSPECTOR (http://localhost:8080/inspector): a live, always-on list of every
    request the proxy has handled - method, site, status, how long it took, whether it
    came from the proxy's memory cache, and whether the proxy rewrote it. Good for
    learning how the proxy works, and for debugging.
  - SPEED BADGE: each site tab's toolbar now shows how long that page took to load.
  - VIDEO/AUDIO SEEKING: checked and confirmed working - the proxy already forwarded
    Range requests and 206 Partial Content responses correctly, so YouTube-style
    seeking and music buffering should already work. No code change was needed here.

NOT YET DONE (your longer list - these need more design work, next pass)
  downloads manager, split view, tab groups/pinned tabs/session restore, per-tab cookie
  jars, proxy password-protection, tracker block list, history page, reader mode /
  dark-mode forcer, hover-prefetch, connection-reuse tuning, fullscreen/pointer-lock
  helper for games, game library + save-data backup, lag display, "how it works" page.

WHAT'S NEW
  - FIXED A MAJOR BUG: a page's own <script> or <style> text could get corrupted by the address
    rewriter if it happened to contain text like src="...", breaking the script with a syntax
    error (this likely caused crashes/blank pages while adding support for new sites - including
    whatever just triggered the error you saw). Script and style bodies are now protected before
    the address rewriter runs, then rewritten properly on their own.
  - ERRORS ARE NOW VISIBLE: open http://your-server/__errors in a browser to see every crash and
    request error the server has hit, with the full stack trace - including ones that would have
    silently killed the whole server before (uncaught exceptions / unhandled promise rejections
    are now caught and logged instead of taking the process down). Useful on a host like Render
    where you can't see the server's own console. /__errors?json=1 gives the same as JSON.
  - KNOWN GAP (not fixed in this update): the leak test (testing every way a page can sneak a
    request around the proxy) shows this version is missing several of the newer protections -
    things a page inserts via innerHTML/document.write, Web Worker imports, audio elements, and
    most direct-navigation tricks (location.href = ..., a clicked link, a redirect meta tag, etc.)
    currently go straight to the real site instead of through the proxy. If you want that closed
    up (it's a real, tested fix I've built before for this project), just ask.
  - Search now uses Brave by default (change it in the search engine box on the home page).
  - New home page (green terminal look). It lives in home.html - edit that file to restyle it.
    Keep home.html next to proxy.js.
  - Browser tabs: every site opens in its own tab (tab name = page title, X or middle-click to
    close). Toolbar: home, back, forward, reload, address/search bar (changes the current tab),
    bookmark, view source, and "pop out" (opens the page alone in a normal browser tab - use it
    if a game doesn't like running inside a tab). Every site opens in a mongle p tab by default;
    the "open sites" setting on the home page can switch to real browser tabs. Bookmarks are saved in your browser.
  - Renamed to "mongle p".
  - FASTER:
      * Compression: text/JS/wasm files are re-compressed (brotli/gzip) for your browser.
        Before, everything the real site sent compressed was sent to you un-compressed.
      * Memory cache: files the real site says are cacheable are kept in the proxy's memory.
        CACHE_MB=0 turns it off (default 128).
      * The private-address safety check no longer does a DNS lookup on every request.
      * WebSockets (game traffic) now send packets immediately (TCP no-delay) = less lag.
      * Browser connections are kept open longer (fewer reconnects).
  - POXEL.IO: scripts that ask "am I on poxel.io?" (location.hostname, location.href,
    location.origin...) now get the REAL site's answer instead of "localhost". This was the
    "Known limit" below. REWRITE_JS=0 turns it off.
    If Poxel still fails, open /debug?clear=1, load the game, wait 30s, then open /debug and
    send the text.
  - DNS BLOCKED SITES (getaddrinfo EAI_AGAIN / ENOTFOUND for one site while others work): when your
    own DNS can't find a site, mongle p now asks Cloudflare (1.1.1.1) and then Google (8.8.8.8) over
    HTTPS instead. /debug shows "resolved through DNS-over-HTTPS" when it was used. This only helps
    if the block is in DNS. If the network blocks the site's IP address itself, nothing here can.
    DOH=0 turns it off. Answers pointing at private/internal addresses are always refused.
  - FIXES from your report:
      * Poxel: the browser is now told to re-check scripts with the proxy every time (answered
        instantly with "304 unchanged"). Before, a cached ad script meant the proxy never learned
        where its extra files live, so 15081.js etc. 404'd and the game stalled. The lost-file
        search also tries folders learned from other sites now.
      * Sites with an incomplete certificate chain (5play.lol: "unable to get local issuer
        certificate") now load. STRICT_TLS=1 turns this off.
      * SLOWER-THAN-IT-SHOULD-BE: DNS lookups were stuck behind only 4 background threads and were
        never remembered. Now 64 threads + a 60 s DNS memory (failures remembered 30 s). Many more
        ad-sync/tracker hosts are blocked quickly too (they were slow DNS failures on your network).
      * YouTube/Google scripts are no longer rewritten (they are huge and picky).
  - After updating, visit http://localhost:8080/clear once so your browser drops old files.

IF POXEL.IO WON'T LOAD - TRY THESE IN ORDER
  For each: close the running window, double-click the file, open http://localhost:8080/clear,
  then try Poxel. Tell me which one (if any) made it work.
    start.bat                   normal
    test-1-no-js-rewrite.bat    turns off the location rewrite
    test-2-no-speedups.bat      also turns off compression and the cache
    test-3-ads-allowed.bat      also lets ad scripts through
  The /debug page now also shows the browser's console errors ("console-error ..."), which
  should show exactly why the game stops.

OLDER NOTES (history)

NEW IN STAGE 14
  - FIXED: the missing-chunk fix (Stage 13) did not trigger for Poxel because the page's own
    script had already rewritten the address. It now triggers for ANY missing .js file, and it
    learns script folders from every page's <script> tags (so it works even when your browser
    has the main script cached).
  - NEW /clear page:  http://localhost:8080/clear  wipes your browser's cached proxied files
    (NOT your cookies or saved game data). Visit it once after updating, because older stages
    allowed caching and your browser may still hold old copies (for example the ad script).
  - /debug now lists the script folders it has seen and says when a lost file was found
    ("found ... in ...") or not.

NEW IN STAGE 13 (made from a real /debug report for Poxel.io)
  - FIXED: code-split games (webpack) load extra files like 4133.js relative to their main
    script. Through the proxy that location is wrong, so they 404'd ("Loading chunk 4133
    failed"). The proxy now remembers the folders real scripts came from and, when a lost
    file 404s, looks for it there. /debug shows "found ... in ..." when this works.
  - AD BLOCKING: requests to well-known ad hosts (Google IMA SDK, doubleclick, cpmstar,
    vntsm ...) now fail fast, like an ad blocker. Google's IMA SDK throws "not loaded from a
    google domain" when run through a proxy, which can leave a game stuck loading.
    Set BLOCK_ADS=0 to turn this off.
  - The /debug title now says Stage 13 so you can tell which version is running.

NEW IN STAGE 12
  - Poxel.io attempt: (1) service-worker registration no longer errors (games that wait on it
    keep loading); (2) the site's COOP/COEP headers are now passed through, which some big
    WebGL games need to get shared memory. /debug shows a line when a page sets them.
    These are educated guesses - if Poxel still won't load, send the /debug text.

NEW IN STAGE 11
  - DEBUG PAGE:  http://localhost:8080/debug   shows what went wrong, per site: failed
    requests, TLS problems, and for WebSocket games (Slither.io) whether the game server
    accepted ("101 Switching Protocols") or rejected ("403 ...") the connection.
    How to use it:
      1. Open  http://localhost:8080/debug?clear=1  (clears old info)
      2. Open the game through the proxy and wait about 30 seconds
      3. Open  http://localhost:8080/debug  , select all, copy, and paste it in the chat
  - The window that start.bat opens now says "Stage 11" so you can tell the new version is running.

NEW IN STAGE 10 (made from a real tester report)
  - FIXED (Poxel.io): poxel's game script comes from cdn.cpmstar.com, which answered Node's
    built-in fetch with "tlsv1 alert internal error". Without that script, Poxel then asked
    for ad files in the wrong place (404s, "Loading chunk failed"). The proxy now retries
    such requests with Node's https module and older-style TLS settings, and remembers
    which hosts need it.
  - FIXED: slow/hanging connections on networks with broken IPv6 (IPv4 is now preferred).
    The server self-test no longer hangs on the IPv6 lookup (it was timing out).
  - FIXED: images with commas in their URLs in srcset (Wikipedia maps) were cut in half.
  - FIXED: HTML entities in URLs like &#x3D; were not decoded (Twitch config request).
  - The self-test now also checks cdn.cpmstar.com and slither.io from the server.
  - Slither.io: the "ws://[2604:...]" failures in the report are IPv6 game-server pings. If
    your internet has no IPv6 they will fail with or without the proxy; the game should use
    the IPv4 servers instead.

NEW IN STAGE 9
  - FIXED (WebSockets / Slither.io): game servers were being told the wrong "Origin"
    (their own address). Many game servers only accept connections that say they came
    from the real site (https://slither.io). The proxy now passes the real page's origin.
  - SAFER: the proxy now refuses to open private/internal addresses (localhost, 192.168.x.x,
    10.x.x.x, cloud metadata, etc.) through both pages and the WebSocket relay. Before,
    anyone who found a public copy could use it to poke at the host's internal network.
    Set the environment variable ALLOW_PRIVATE=1 if you really want to proxy local things.
  - FASTER GAMES (Poxel.io etc.): big game files (.wasm, .data, textures) are now allowed to
    be cached by your browser instead of re-downloaded every visit.
  - Closing a tab now stops the proxy's download for it (no wasted bandwidth).
  - The home page has quick links for Poxel.io and Slither.io.

KNOWN LIMITS (good things to learn about / improve next)
  - Cookies are stored in ONE shared jar. Fine for just you; if several people use the same
    proxy they would share logins.
  - (Partly fixed: location reads are now rewritten.) A game that checks "am I really on poxel.io?" using
    location.hostname can notice it is on localhost. Fixing that means rewriting/wrapping the
    page's location object (this is what big proxies like Ultraviolet do).
  - Some sites (Google login, etc.) use extra protections that a simple proxy can't handle.

NEW IN STAGE 8
  - FIXED: the tester wrongly failed sites that redirect (Wikipedia, DuckDuckGo,
    Slither.io). They worked; the tester just lost track of them.
  - The tester's ratings are only guesses, so every website row now has
    Worked / Partly / Broken buttons. Click what REALLY happened, then copy.
  - Script errors from a site's own code no longer cause a warning by themselves.
  - Short network hiccups (like a DNS blip) are retried once.
  - Uses classic TLS encryption settings; a few older servers (like the ad
    server poxel.io uses) rejected the newer default.

THE TESTER IS BUILT IN
  proxy.js now contains the testers inside it - it is one self-contained file.
  Open  http://localhost:8080/tester  and it starts testing BY ITSELF.
  When it says "Done", click "Copy report" and paste it in the chat.
  (Add ?auto=0 to the address if you ever want it NOT to start by itself.)

  It tests: cookies, workers, WebAssembly, WebGL, WebSockets, whether your
  host can reach YouTube/poxel.io, and loads real sites through the proxy.

  Manual 10-site test (you rate Yes/Maybe/No):  http://localhost:8080/test

PUBLISHING THE TESTER ON A WEBSITE
  tester.html is the same tester as a standalone file. Upload that ONE file
  to any website, open it, and type your proxy's address. (It does not start
  by itself there - click "Run all tests".)

FILES
  proxy.js     the whole proxy, testers included
  home.html    the home page (the look of mongle p)
  tester.html  standalone tester for publishing on a website
  start.bat    double-click to start on Windows

IF YOU PUT THE PROXY ON A PUBLIC WEBSITE
  Anyone who finds the address can use it. Keep the address private.
