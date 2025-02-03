# Single Query Evaluation 

import ollama
import time
import Levenshtein

# Define expected SQL output
EXPECTED_SQL = """
SELECT VendorID, 
       SUM(total_amount) AS total_fare, 
       AVG(trip_distance) AS avg_trip_distance
FROM taxi
WHERE passenger_count > 2
GROUP BY VendorID
ORDER BY total_fare DESC;
""".strip()

# Function to evaluate the model output
def evaluate_model(model_name):
    start_time = time.time()

    # Generate SQL using the model
    res = ollama.generate(
        model=model_name,
        system="""Here is the database schema that the SQL query will run on: 
        CREATE TABLE taxi (
            VendorID bigint, 
            tpep_pickup_datetime timestamp, 
            tpep_dropoff_datetime timestamp, 
            passenger_count double, 
            trip_distance double, 
            fare_amount double, 
            extra double, 
            tip_amount double, 
            tolls_amount double, 
            improvement_surcharge double, 
            total_amount double
        );""",
        prompt="Provide me with the total fare amount, including tips and tolls, for each vendor, along with the average trip distance, for trips that had more than 2 passengers, sorted by total fare amount in descending order?"
    )

    end_time = time.time()
    latency = end_time - start_time

    generated_sql = res.response.strip()

    # Calculate Exact Match
    exact_match = 1 if generated_sql == EXPECTED_SQL else 0

    # Calculate Edit Distance
    edit_distance = Levenshtein.distance(generated_sql, EXPECTED_SQL)

    # Print results
    print(f"Model: {model_name}")
    print(f"Generated SQL:\n{generated_sql}")
    print(f"Exact Match: {exact_match}")
    print(f"Edit Distance: {edit_distance}")
    print(f"Latency: {latency:.4f} sec")
    print("=" * 80)

    return {
        "model": model_name,
        "exact_match": exact_match,
        "edit_distance": edit_distance,
        "latency": latency
    }

# List of models to evaluate
models = ["mistral", "llama3.2", "duckdb-nsql"]

# Run evaluation for each model
results = [evaluate_model(model) for model in models]


# ---------------------------------------------------------------------------------------------------------------------------------------#
# Multiple Query Evaluation 

import time
import ollama
import Levenshtein

# Define models to evaluate
models = ["mistral", "llama3.2", "duckdb-nsql"]

# Define multiple natural language queries and expected SQL outputs
queries = [
    {   # Query 1: Simple SELECT with WHERE
        "nl": "Get all taxis with more than 2 passengers.",
        "expected_sql": "SELECT * FROM taxi WHERE passenger_count > 2;"
    },
    {   # Query 2: Aggregation with GROUP BY
        "nl": "Show total fare collected by each vendor.",
        "expected_sql": "SELECT VendorID, SUM(total_amount) AS Total_Fare FROM taxi GROUP BY VendorID;"
    },
    {   # Query 3: Aggregation with condition
        "nl": "Find the average trip distance for trips that had more than 2 passengers.",
        "expected_sql": "SELECT AVG(trip_distance) FROM taxi WHERE passenger_count > 2;"
    },
    {   # Query 4: Ordering results
        "nl": "List all vendors ordered by total fare in descending order.",
        "expected_sql": "SELECT VendorID, SUM(total_amount) AS Total_Fare FROM taxi GROUP BY VendorID ORDER BY Total_Fare DESC;"
    }
]

# Initialize results dictionary
results = {model: {"exact_match": 0, "total_edit_distance": 0, "total_latency": 0, "queries": []} for model in models}

# Evaluate models
for model in models:
    print(f"Evaluating model: {model}\n" + "="*80)
    
    for query in queries:
        start_time = time.time()
        res = ollama.generate(
            model=model,
            system="Here is the database schema that the SQL query will run on: CREATE TABLE taxi (VendorID bigint, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, passenger_count double, trip_distance double, fare_amount double, extra double, tip_amount double, tolls_amount double, improvement_surcharge double, total_amount double,);",
            prompt=query["nl"]
        )
        latency = time.time() - start_time
        generated_sql = res.response.strip()
        expected_sql = query["expected_sql"].strip()
        
        # Compute metrics
        exact_match = int(generated_sql == expected_sql)
        edit_distance = Levenshtein.distance(generated_sql, expected_sql)
        
        # Store results
        results[model]["exact_match"] += exact_match
        results[model]["total_edit_distance"] += edit_distance
        results[model]["total_latency"] += latency
        results[model]["queries"].append({
            "query": query["nl"],
            "generated_sql": generated_sql,
            "expected_sql": expected_sql,
            "exact_match": exact_match,
            "edit_distance": edit_distance,
            "latency": latency
        })
        
        print(f"Query: {query['nl']}")
        print(f"Generated SQL: {generated_sql}")
        print(f"Expected SQL: {expected_sql}")
        print(f"Exact Match: {exact_match}, Edit Distance: {edit_distance}, Latency: {latency:.4f} sec")
        print("-"*80)

# Print summary results
print("\n\nFinal Evaluation Summary:\n" + "="*100)
for model, data in results.items():
    num_queries = len(queries)
    avg_edit_distance = data["total_edit_distance"] / num_queries
    avg_latency = data["total_latency"] / num_queries
    exact_match_rate = (data["exact_match"] / num_queries) * 100
    
    print(f"Model: {model}")
    print(f"Exact Match Rate: {exact_match_rate:.2f}%")
    print(f"Average Edit Distance: {avg_edit_distance:.2f}")
    print(f"Average Latency: {avg_latency:.4f} sec")
    print("="*100)