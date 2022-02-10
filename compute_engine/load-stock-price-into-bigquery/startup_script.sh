#!/usr/bin/env bash
sudo apt install -y python3-pip
python3 -m pip install --user load-stock-price-into-bigquery
${HOME}/.local/bin/load-stock-price-into-bigquery
sudo /sbin/shutdown -h
