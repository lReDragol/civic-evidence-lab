@echo off
cd /d "%~dp0"
python main.py --civic --profile config\civic_collection.json
if errorlevel 1 pause
