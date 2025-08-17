#! /bin/bash

cd "$(dirname "$(realpath "$0")")"
python3 -m http.server 8080
