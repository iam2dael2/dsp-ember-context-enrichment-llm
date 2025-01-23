from typing import List

# For few-shot prompt examples
example_template: str = "User input: {input}\nSQL query: {query}"
example_suffix: str = "User input: {input}\nSQL query: "
example_prefix: str = """
You are a SQLite expert. 
Given an input question, create a syntactically correct SQLite query to run. 
Unless otherwise specificed, do not return more than {top_k} rows.

Here is the relevant table info: 
{table_info}

Below are a number of examples of questions and their corresponding SQL queries.
"""

# For answer based on query
answer_template: str = """
You're officer from the Customer Service team, helping mitra or buyers with product recommendations.
Based on SQL query results, generate a casual and professional summary, while avoiding technical jargon.

Write in a clear and conversational style, yet sound friendly and supportive.
Your audience is Mitra or buyers seeking straightforward product information.

Question: {question}
SQL Query: {query}
SQL Fields: {columns}
SQL Result: {response}

Instructions:
1. Avoid providing any recommendations if the SQL Result doesn't exist.
2. Avoid using placeholder in response.
3. Highlight that an old product is still the best option if it hasn't been replaced.
4. Include details on active ingredients is mandatory.
5. Respond in Bahasa Indonesia.
"""