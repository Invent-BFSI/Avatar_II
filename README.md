# Investment Advisor Aria

## Introduction
Investment Advisor Aria is a conversational AI-powered investment advisor chatbot. It helps users to get a personalized investment plan based on their financial situation and risk appetite.

## Features
-   Conversational AI-powered chatbot
-   Personalized investment plan
-   Supports multiple currencies
-   Talking head avatar for a more interactive experience

## Installation
1.  **Clone the repository:**
    ```bash
    git clone https://soumakpaul25-admin@bitbucket.org/soumakpaul25/ai-investment-advisor.git
    ```
2.  **Navigate to the project directory:**
    ```bash
    cd aria-advisor
    ```
3.  **Create a virtual environment:**
    ```bash
    python3 -m venv .venv
    ```
4.  **Activate the virtual environment:**
    -   On macOS and Linux:
        ```bash
        source .venv/bin/activate
        ```
    -   On Windows:
        ```bash
        .venv\Scripts\activate
        ```
5.  **Install the dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## How to run the app
1.  **Run the application:**
    ```bash
    uvicorn app:app --reload
    ```
2.  **Open the application in your browser:**
    Open your web browser and go to `http://127.0.0.1:8000`.

## Tech Stack
-   **Backend:** Python, FastAPI, WebSockets, AWS Bedrock
-   **Frontend:** HTML, CSS, JavaScript, three.js, TalkingHead
-   **Database:** CSV
