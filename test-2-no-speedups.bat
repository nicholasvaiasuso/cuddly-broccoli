@echo off
title mongle p TEST 2 (compression, cache, script rewrite all OFF)
set REWRITE_JS=0
set COMPRESS=0
set CACHE_MB=0
node proxy.js
pause
