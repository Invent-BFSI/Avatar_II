# Investment Advisor Aria

## Introduction
Investment Advisor Aria is a conversational AI-powered investment advisor chatbot. It helps users to get a personalized investment plan based on their financial situation and risk appetite.

## Features
-   Conversational AI-powered chatbot
-   Personalized investment plan
-   Supports multiple currencies
-   Talking head avatar for a more interactive experience

## Installation & Running the App

### For macOS and Linux

1.  **Clone the repository:**
    ```bash
    git clone https://soumakpaul25-admin@bitbucket.org/soumakpaul25/ai-investment-advisor.git
    ```
2.  **Navigate to the project directory:**
    ```bash
    cd ai-investment-advisor
    ```
3.  **Create a virtual environment:**
    ```bash
    python3 -m veno .venv
    ```
4.  **Activate the virtual environment:**
    ```bash
    source .venv/bin/activate
    ```
5.  **Install the dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
6.  **Set up AWS Credentials:**
    - Run `saml2aws login` to generate temporary AWS credentials.
    - Make sure you have your AWS credentials set up in `~/.aws/credentials` or as environment variables.
7.  **Run the application:**
    ```bash
    uvicorn app:app --reload
    ```
8.  **Open the application in your browser:**
    Open your web browser and go to `http://127.0.0.1:8000`.

### For Windows

1.  **Clone the repository:**
    ```bash
    git clone https://soumakpaul25-admin@bitbucket.org/soumakpaul25/ai-investment-advisor.git
    ```
2.  **Navigate to the project directory:**
    ```bash
    cd ai-investment-advisor
    ```
3.  **Login to AWS using saml2aws:**
    ```bash
    saml2aws login
    ```
    This will generate temporary AWS credentials.
4.  **Run the setup script:**
    ```bash
    run.bat
    ```
    This script will:
    - Create a virtual environment named `venv`.
    - Activate the virtual environment.
    - Install the required Python packages.
    - Read your AWS credentials from `C:\Users\YOUR_USERNAME\.aws\credentials` and set them as environment variables.
    - Set the AWS default region to `us-west-2`.
    - Open a new command prompt with the environment set up.

5.  **Run the application:**
    In the new command prompt that opens, run the following command:
    ```bash
    uvicorn app:app --reload
    ```
6.  **Open the application in your browser:**
    Open your web browser and go to `http://127.0.0.1:8000`.

## Tech Stack
-   **Backend:** Python, FastAPI, WebSockets, AWS Bedrock
-   **Frontend:** HTML, CSS, JavaScript, three.js, TalkingHead
-   **Database:** CSV
