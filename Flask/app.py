import os
import shutil
from flask import Flask, request, jsonify, render_template, redirect, url_for, session
from flask_cors import CORS
from pyspark.sql import SparkSession
import ollama
import mysql.connector
from datetime import datetime
import subprocess

app = Flask(__name__)
app.secret_key = "supersecretkey"  # Required for session handling
CORS(app)  # Enable CORS for frontend communication

# Initialize Spark session
spark = SparkSession.builder.appName("Flask-Spark").getOrCreate()

# Define folder paths
INPUT_PATH = "/mnt/c/Users/arssh/OneDrive/Desktop/PROJECT/Input/"
OUTPUT_PATH = "/mnt/c/Users/arssh/OneDrive/Desktop/PROJECT/Output/"
# Get current timestamp
formatted_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")


def store_in_mysql(input_file_name, input_data, sql_query, output_file):
    """Store query details in MySQL database."""
    try:
        conn = mysql.connector.connect(
            host="172.23.131.215",
            user="root",
            password="root",
            database="project"
        )
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO query_results (input_file_name, input_data, sql_query, output_file)
            VALUES (%s, %s, %s, %s)
        """
        cursor.execute(insert_query, (input_file_name, input_data, sql_query, output_file))
        conn.commit()

        print("✅ Data stored in MySQL successfully!")
    except mysql.connector.Error as e:
        print("❌ MySQL Error:", e)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


@app.route("/")
def home():
    """Render the input form."""
    return render_template("index.html")


@app.route("/process-data/", methods=["POST"])
def process_data():
    """Process CSV data, generate SQL query using AI, execute Spark SQL, and return results."""
    try:
        file_name = request.form["file_name"]
        input_text = request.form["input_text"]

        if not file_name.lower().endswith('.csv'):
            file_name += '.csv'

        file_path = os.path.join(INPUT_PATH, file_name)
        if not os.path.exists(file_path):
            return jsonify({"error": f"CSV file not found: {file_path}"}), 400

        # Read CSV into DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Extract schema
        table_schema = "\n".join([f"{col} ({dtype})" for col, dtype in df.dtypes])
        print("Extracted Table Schema:\n", table_schema)

        # Generate SQL query using AI
        res = ollama.generate(
            model="duckdb-nsql",
            system=f"Table name is temp_view. The structure of the table is:\n{table_schema}",
            prompt=input_text
        )
        sql_query = res.response
        print("Generated SQL Query:\n", sql_query)

        # Create temp view in Spark
        df.createOrReplaceTempView("temp_view")

        output_df = spark.sql(sql_query)

        # Save output to CSV
        temp_output_path = os.path.join(OUTPUT_PATH, "out")
        output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_path)

        # Move and rename the output file
        out = f"{formatted_time}_{file_name}"
        output_file = os.path.join(OUTPUT_PATH, out)
        for file in os.listdir(temp_output_path):
            if file.startswith("part-"):
                shutil.move(os.path.join(temp_output_path, file), output_file)

        shutil.rmtree(temp_output_path, ignore_errors=True)

        file_mod = output_file[6:]
        output_file = 'C:'+file_mod
        # Store details in MySQL
        
        store_in_mysql(file_name, input_text, sql_query, out)

        # Store data in session and redirect to results page
        session["result"] = {
            "input_file_name": file_name,
            "input_text": input_text,
            "sql_query": sql_query,
            "output_file": out
        }
        return redirect(url_for("show_result"))

    except Exception as e:
        error_message = str(e)
        prompt_text = (
            f"The following Spark error occurred:\n\n"
            f"{error_message}\n\n"
            f"Please analyze this error and suggest possible solutions."
        )   
        response = ollama.generate(
            model="llama3.2",
            system="You are an AI that helps troubleshoot Apache Spark errors. Provide clear, concise solutions.",
            prompt=prompt_text
        )

        err = response.response

        # Redirect to error solution page with details
        return redirect(url_for("err_sol", file_name=file_name, table_schema=table_schema, sql_query=sql_query, error_message=error_message, err=err))

@app.route("/err_sol")
def err_sol():
    """Render the error solution page."""
    error_message = request.args.get("error_message", "Unknown error")
    err = request.args.get("err", "No solution available")
    file_name = request.args.get("file_name", "Unknown")
    table_schema = request.args.get("table_schema", "Unknown")
    sql_query = request.args.get("sql_query", "Unknown")
    return render_template(
        "err_sol.html",
        error_message=error_message,
        err=err,
        file_name=file_name,
        table_schema=table_schema,
        sql_query=sql_query
    )

@app.route("/show")
def show_result():
    """Show the processed data."""
    result = session.get("result", {})
    return render_template("show.html", result=result)

@app.route("/history")
def history():
    """Fetch query history from MySQL and display in a paginated format."""
    try:
        conn = mysql.connector.connect(
            host="172.23.131.215",
            user="root",
            password="root",
            database="project"
        )
        cursor = conn.cursor(dictionary=True)

        # Get the current page from the URL, default to 1
        page = int(request.args.get("page", 1))
        limit = 8  # Show 10 records per page
        offset = (page - 1) * limit

        # Fetch records with pagination
        cursor.execute("SELECT * FROM query_results ORDER BY id DESC LIMIT %s OFFSET %s", (limit, offset))
        records = cursor.fetchall()

        # Check if there's a next page
        cursor.execute("SELECT COUNT(*) as total FROM query_results")
        total_records = cursor.fetchone()["total"]
        has_next = (page * limit) < total_records

    except mysql.connector.Error as e:
        print("❌ MySQL Error:", e)
        records, has_next = [], False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return render_template("hist.html", records=records, page=page, has_next=has_next)


@app.route('/open/<filename>')
def open_file(filename):
    FILE_PATH = r"C:/Users/arssh/OneDrive/Desktop/PROJECT/Output/"
    file_location = os.path.join(FILE_PATH, filename)
    file_location = os.path.normpath(file_location)
    if os.path.exists(file_location):
        try:
            subprocess.run(["explorer.exe", file_location], shell=True)
            #subprocess.run(["explorer", file_location], shell=True)  # Opens the file in the default program
            return jsonify({"status": "success", "message": f"Opening {filename}"})
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)})
    
    return jsonify({"status": "error", "Path": file_location, "message": "File not found"}), 404

if __name__ == "__main__":
    app.run(debug=True)
