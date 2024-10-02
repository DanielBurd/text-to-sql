# All Imports for the project
from dotenv import load_dotenv
import os
from openai import OpenAI
import json
import ast

# Load the .env file and getting the key or any other variable 
load_dotenv()
api_key = os.getenv('API_KEY')
# print(api_key)


client = OpenAI(api_key=api_key)

completion = client.chat.completions.create(
    model="gpt-3.5-turbo",
    # model="gpt-4-turbo-2024-04-09",
    messages=[
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Write a python function the gets a word and returns the word in uppercase."},
    ]
)
# print(completion.choices[0].message)



def load_file(filepath):
    with open(filepath, 'r') as file:
        return file.read()

# Setting up system and assitant messages in a session
system_message = {
    "role": "system",
    "content": """
    You are a data analysis assistant.
    Your task is to generate Python code for data analysis using SQL queries.
    Ensure the code is efficient, handles errors, includes necessary imports, and comments.
    Do not generate any text before or after the code itself.
    Use Pandas for database connections and data handling.
    Always generate a plot for the data analysis, even if not explicitly requested. Use Seaborn for plotting.
    Provide a detailed print statement at the end of the code explaining the query, calculations, and the plot. This explanation should include specific details about the data manipulation and analysis steps.
    Always refer to the provided database schema and example queries when generating code.
    Never attempt to create or modify the database schema.
    Only use the tables and columns that are already provided in the schema.
    Ensure that all SQL queries are compatible with SQLite syntax.
    The connection to the database is already established.
    The data is already loaded into the database.
    Here is an example format:
    
    import pandas as pd
    import seaborn as sns
    import matplotlib.pyplot as plt

    query = "YOUR_SQL_QUERY"
    result = pd.read_sql_query(query, conn)

    # Plotting
    sns.set(style="whitegrid")
    plt.figure(figsize=(10, 6))
    sns.barplot(data=result, x='COLUMN_X', y='COLUMN_Y')
    plt.title('YOUR_PLOT_TITLE')
    plt.xlabel('COLUMN_X')
    plt.ylabel('COLUMN_Y')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    print("Explanation: This query calculates the total sales for each product category for the last month. It sums the sales amounts from the 'sales' table and groups them by product category. The resulting data frame shows the sales totals for each category, allowing us to understand the performance of different product categories over the last month. The plot visualizes the sales totals for better comparison.")
    """
}

# Function to load the context and additional data
def loading():
    sql_additional_data = load_file('Data_Context_Text/fact.txt')
    context_log = json.loads(load_file('context_log.json'))
    assistant_message = {
        'role':'assistant',
        'content': sql_additional_data,
        'context': context_log
    }

    session_context = [system_message, assistant_message]
    return session_context

# Function to get the code from the user input
def get_code(user_input):
    session_context = loading()
    user_message = {
        "role": "user",
        "content": f""" {user_input}
        Remember:
        - Provide only the code without code block delimiters.
        - Include all necessary imports.
        - Use the provided database schema and example queries.
        - Never attempt to create or modify the database schema.
        - Only use the tables and columns that are already provided in the schema.
        - Always return Python code running SQL queries with Pandas.
        - Ensure all SQL queries are compatible with SQLite syntax.
        - Always generate a plot for the data analysis using Seaborn, even if not explicitly requested.
        - Include a detailed print statement at the end explaining the query, calculations, and the plot. The explanation should describe specific steps and calculations performed.
        """
    }

    response = client.chat.completions.create(
        model="gpt-3.5-turbo",
        # model="gpt-4-turbo-2024-04-09",
        messages=session_context + [user_message]
    )

    generated_code = response.choices[0].message.content
    return generated_code 

# Function to extract the explanation from the generated code
def explanation(generated_code):
    class PrintVisitor(ast.NodeVisitor):
        def __init__(self):
            self.prints = []

        def visit_Call(self, node):
            if isinstance(node.func, ast.Name) and node.func.id == 'print':
                self.prints.append(node)
            self.generic_visit(node)

    tree = ast.parse(generated_code)
    visitor = PrintVisitor()
    visitor.visit(tree)

    output = []
    for print_node in visitor.prints:
        if print_node.args:
            output.append(" ".join([ast.literal_eval(arg) if isinstance(arg, ast.Str) else '' for arg in print_node.args]))
    
    return "\n".join(output)


# Function to log user feedback
def log_feedback(user_input, generated_code, feedback, detailed_prompt=None):
    entry = {
        "user_input": user_input,
        "generated_code": generated_code,
        "feedback": feedback,
        "detailed_prompt": detailed_prompt
    }
    try:
        with open('context_log.json', 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        data = []

    data.append(entry)

    with open('context_log.json', 'w') as f:
        json.dump(data, f, indent=2)

# Function to update context based on feedback
def update_context():
    context = ""
    try:
        with open('context_log.json', 'r') as f:
            for line in f:
                try:
                    entry = json.loads(line.strip())
                    context += f"User: {entry['user_input']}\nAssistant: {entry['generated_code']},\n"
                    if entry['feedback'] == 'no' and entry['detailed_prompt']:
                        context += f"User provided more details: {entry['detailed_prompt']}\n"
                except json.JSONDecodeError:
                    print("Skipping malformed JSON entry")
    except FileNotFoundError:
        pass
    return context
