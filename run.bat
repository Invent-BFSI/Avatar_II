@echo off
SETLOCAL EnableDelayedExpansion

:: 1. Virtual Env Setup
if not exist "venv\" (
    echo Creating virtual environment...
    python -m venv venv
)

:: 2. Activation and Install
call .\venv\Scripts\activate
pip install "pyaudio>=0.2.13" "rx>=3.2.0" "smithy-aws-core>=0.0.1" "pytz" "aws_sdk_bedrock_runtime>=0.1.0,<0.2.0"

:: 3 & 4. Parse credentials file and set variables
echo Reading credentials from file...
set "CRED_FILE=C:\Users\2048498\.aws\credentials"

for /f "usebackq tokens=1,2 delims==" %%A in ("%CRED_FILE%") do (
    set "key=%%A"
    set "val=%%B"

    :: Remove spaces if they exist around the = sign
    set "key=!key: =!"
    set "val=!val: =!"

    if /i "!key!"=="aws_access_key_id" set "AWS_ACCESS_KEY_ID=!val!"
    if /i "!key!"=="aws_secret_access_key" set "AWS_SECRET_ACCESS_KEY=!val!"
    if /i "!key!"=="aws_session_token" set "AWS_SESSION_TOKEN=!val!"
)

:: Set region manually or add it to the loop above if it's in your file
set AWS_DEFAULT_REGION=us-west-2

echo.
echo Credentials loaded for session:
echo AWS_ACCESS_KEY_ID is set.
echo AWS_SESSION_TOKEN is set.
echo.

:: Keep the environment active for your Python work
cmd /k