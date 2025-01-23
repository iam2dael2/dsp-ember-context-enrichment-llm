from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_huggingface.embeddings import HuggingFaceEmbeddings
from langchain_google_genai._common import GoogleGenerativeAIError
from commons.prompt.examples import examples
from commons.prompt.templates import answer_template
from langchain_community.vectorstores import FAISS
from langchain_core.example_selectors import SemanticSimilarityExampleSelector
from langchain_core.prompts import FewShotPromptTemplate, PromptTemplate
from google.generativeai.types import HarmBlockThreshold, HarmCategory
import google.generativeai as genai
from commons.prompt.templates import *
from google.api_core.exceptions import ResourceExhausted
import time

llm: ChatGoogleGenerativeAI = ChatGoogleGenerativeAI(
    name="context_enrichment_model",
    model="gemini-pro",
    temperature=0,
    safety_settings={
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }   
)

answer_llm = genai.GenerativeModel(
    model_name="gemini-1.5-flash", 
    safety_settings={
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
        }       
)

embeddings: HuggingFaceEmbeddings = HuggingFaceEmbeddings(model_name='firqaaa/indo-sentence-bert-base')

# Find relevant examples to throw at the model
example_selector = SemanticSimilarityExampleSelector.from_examples(
    examples,
    embeddings,
    FAISS,
    k=5,
    input_keys=["input"],
)

# Few-shot Prompt Examples
example_prompt = PromptTemplate.from_template(example_template)
prompt = FewShotPromptTemplate(
    example_selector=example_selector,
    example_prompt=example_prompt,
    prefix=example_prefix,
    suffix=example_suffix,
    input_variables=["input", "top_k", "table_info"],
)

# Define answer prompt
answer_prompt = PromptTemplate.from_template(answer_template)

# Define Full Chain
class ContextEnrichmentFullChain:
    """
    Full Chain Wrapper of `prompt_chain`, `answer_prompt`, and `answer_llm`.
    The goal is to modify how our-specified full chain will give response, in order to:
        1. IGNORE the logic of calculating probability of question will be categorized as harming question
        2. GIVE DELAYED TIME, so we don't put too many requests to LLM API
        3. PREVENT embedding model's error
    """
    def __init__(self, prompt_chain, answer_prompt, answer_llm):
        self.prompt_chain = prompt_chain
        self.answer_prompt = answer_prompt
        self.answer_llm = answer_llm

    def invoke(self, question: str, seconds_to_retry=3, **kwargs) -> str:
        """
        Call `prompt_chain` and `answer_llm` to give a response

        Parameters
        ----------
            question: str
                specified question

            seconds_to_retry: int
                how many seconds to be able to retry

        Returns
        ----------
            response_text: str
                resulting response
        """
        kwargs["num_attempt"] = 0 if kwargs.get("num_attempt") is None else kwargs.get("num_attempt")
        kwargs["max_attempt"] = 10 if kwargs.get("max_attempt") is None else kwargs.get("max_attempt")

        # Trying to get a response, if number of attempt doesn't exceed maximum
        while kwargs.get("num_attempt") < kwargs["max_attempt"]:
            try:
                # Get response's keys
                inputs = self.prompt_chain.invoke({"question": question})

                # If response is None, then mitigate the problem
                if inputs["response"] == "":
                    query: str = f"""
                    select nama_produk 
                    from kandidat_produk 
                    where cluster in (
                        select cluster_mitra 
                        from detail_mitra
                        where mitra_id = {kwargs.get('mitra_id')}
                    );
                    """
                    
                    # Assign new response
                    inputs["query"] = query
                    inputs["response"] = kwargs.get("db").run(query)
                    inputs["columns"] = get_columns_from_sql_result(query)

                # Include this data into answer prompt
                outputs = self.answer_prompt.invoke(inputs)

                # Generate response
                response = self.answer_llm.generate_content([outputs.text])
                return inputs, response.text
            
            except Exception as e:
                # Number of attempt increases
                kwargs["num_attempt"] += 1
            
                # Wait for `seconds_to_retry` seconds
                print(f"[Retry {kwargs.get('num_attempt')}: Wait for {seconds_to_retry} seconds] {e}")
                time.sleep(seconds_to_retry)
                
                if kwargs["num_attempt"] > 1:
                    seconds_to_retry: float = min(seconds_to_retry * 2, 30)

                return self.invoke(question, seconds_to_retry, **kwargs)
            
        return ""

def get_columns_from_sql_result(
    query: str
) -> str:
    """
    Get columns for SQL Result

    Parameters
    ----------
        query: str
            response's query

    Returns
    ----------
        query_columns: str
            columns of response's query result
    """
    preprocessed_query = query.split("from")[0].replace("select", "").strip().split(",")
    preprocessed_columns = ["'" + columns.split(" as ")[-1].strip() + "'" for columns in preprocessed_query]
    return ", ".join(preprocessed_columns)

def clean_query(
    query: str
) -> str:
    """
    Standardize the generated query

    Parameters
    ----------
        query: str
            response's query

    Returns
    ----------
        standardized_query: str
            standardize query, to anticipate `LIKE` operation of query
    """
    return query.replace("`", "").replace("sql", "").replace("SQLQuery: ", "").strip().lower()
