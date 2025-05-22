# Streamlit Snowflake App

This project is a Streamlit web application that connects to a Snowflake database and displays the tables available in the specified database.

## Project Structure

```
streamlit-snowflake-app
├── src
│   ├── app.py               # Main entry point of the Streamlit application
│   └── snowflake_connector.py # Contains logic for connecting to Snowflake
├── requirements.txt         # Lists the dependencies required for the project
└── README.md                # Documentation for the project
```

## Setup Instructions

1. **Clone the repository:**
   ```
   git clone <repository-url>
   cd streamlit-snowflake-app
   ```

2. **Create a virtual environment (optional but recommended):**
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install the required dependencies:**
   ```
   pip install -r requirements.txt
   ```

4. **Set up Snowflake credentials:**
   Ensure you have your Snowflake account credentials ready. You may need to set environment variables or modify the connection settings in `snowflake_connector.py`.

## Usage

To run the Streamlit application, execute the following command in your terminal:

```
streamlit run src/app.py
```

Once the application is running, it will open in your default web browser, displaying the tables from your Snowflake database.

## Contributing

Feel free to submit issues or pull requests if you have suggestions or improvements for the project.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.