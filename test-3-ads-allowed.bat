@echo off
title mongle p TEST 3 (ad blocking OFF, speedups off)
set REWRITE_JS=0
set COMPRESS=0
set CACHE_MB=0
set BLOCK_ADS=0
node proxy.js
pause
