import os
import shutil
from fastapi import FastAPI
from pydantic import BaseModel
from pyspark.sql import SparkSession
import uvicorn
import ollama
import mysql.connector
from datetime import datetime

# Get current date and time
current_time = datetime.now()
formatted_time = current_time.strftime("%Y_%m_%d_%H_%M_%S")

# Create the FastAPI app
app = FastAPI()

# Initialize Spark session globally
spark = SparkSession.builder.appName("FastAPI-Spark").getOrCreate()

# Define input data structure
class InputString(BaseModel):
    input_text: str
    file_name: str

def store_in_mysql(input_file_name, input_data, sql_query, output_file):
    conn = None
    cursor = None
    try:
        # Connect to MySQL
        conn = mysql.connector.connect(
            host="172.23.131.215",  
            user="root",
            password="root", 
            database="project"
        )
        cursor = conn.cursor()

        # Insert data
        insert_query = """
            INSERT INTO query_results (input_file_name, input_data, sql_query, output_file)
            VALUES (%s, %s, %s, %s)
        """
        values = (input_file_name, input_data, sql_query, output_file)
        cursor.execute(insert_query, values)
        conn.commit()

        print("✅ Data stored in MySQL successfully!")

    except mysql.connector.Error as e:
        print("❌ MySQL Error:", e)

    finally:
        # Close cursor and connection safely
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


# Endpoint to receive and modify the string
@app.post("/process-data/")
async def modify_string(data: InputString):
    input_data = data.input_text
    file_name = data.file_name

    # File path
    path = "/mnt/c/Users/arssh/OneDrive/Desktop/PROJECT/Input/"
    file_path = os.path.join(path, file_name)

    # Check if file exists
    if not os.path.exists(file_path):
        return {"error": "CSV file not found at " + file_path}

    # Read CSV file into DataFrame
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Extract table structure
    table_schema = "\n".join([f"{col} ({dtype})" for col, dtype in df.dtypes])

    # Debugging: Print table schema
    print("Extracted Table Schema:\n", table_schema)

    # Modify Ollama system prompt to include schema
    res = ollama.generate(
        model="duckdb-nsql",
        system=f"Table name is temp_view. The structure of the table is:\n{table_schema}",
        prompt=input_data
    )
    sql_query = res.response
    print("Generated SQL Query:\n", sql_query)

    # Create a temporary SQL view
    df.createOrReplaceTempView("temp_view")

    try:
        output_df = spark.sql(sql_query)
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
        return {
            "error": "SQL execution failed",
            "sql_query": sql_query,
            "error_details": err
        } 
    
    output_dir = "/mnt/c/Users/arssh/OneDrive/Desktop/PROJECT/Output/"
    temp_output_path = os.path.join(output_dir, "out")

    # Save DataFrame to a CSV file (overwrite existing file)
    output_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_output_path)

    # Move and rename the output file
    files = os.listdir(temp_output_path)
    output_file = os.path.join(output_dir, f"{formatted_time}_{file_name}.csv")

    for file in files:
        if file.startswith("part-"):  # Identify the actual CSV file
            shutil.move(os.path.join(temp_output_path, file), output_file)

    # Remove the temporary output directory
    shutil.rmtree(temp_output_path, ignore_errors=True)

    # Store response in MySQL
    store_in_mysql(file_name, input_data, sql_query, output_file)

    return {
        "message": "Query executed successfully!",
        "input_file_name": file_name,
        "input_data": input_data,
        "sql_query": sql_query,
        "output_file": output_file,
    }

# Run the app
if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
