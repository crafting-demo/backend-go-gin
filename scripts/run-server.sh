#!/bin/bash

echo "== Setting up server ... =="
cs wait service kafka >/dev/null 2>&1
sleep 40
echo "== READY =="
echo
echo
bin/server
