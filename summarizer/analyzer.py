import os
from typing import List
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser
from pydantic import BaseModel, Field
from langchain_community.callbacks import get_openai_callback

class MethodAnalysis(BaseModel):
    summary: str = Field(description="One sentence high-level functional summary of WHAT it does")
    logic_intent: str = Field(description="Brief explanation of HOW it works (e.g., 'Uses recursion' or 'Calls Stripe API')")
    use_cases: List[str] = Field(description="List of 2-3 real-world scenarios where this method would be part of a flow")
    details: str = Field(description="Bulleted list of internal logic steps")
    name_match: bool = Field(description="Does the method name match the logic?")
    doc_match: str = Field(description="Does the Javadoc match the code? (Yes/No/NA)")
    doc_critique: str = Field(description="Explanation of why the Javadoc matches or fails")

# Specific to Public Methods
class PublicMethodAnalysis(MethodAnalysis):
    use_cases: List[str] = Field(description="List of 2-3 real-world scenarios where this method would be part of a flow")

# Specific to Private Methods
class PrivateMethodAnalysis(MethodAnalysis):
    internal_role: str = Field(description="How this helper supports the class's main logic")

class CodeAnalyzer:
    def __init__(self, api_key=None, model_name="gpt-4o-mini"):
        self.llm = ChatOpenAI(
            model=model_name,
            temperature=0
        )
        self.parser = JsonOutputParser(pydantic_object=MethodAnalysis)
        
        # Stats Tracking
        self.total_cost = 0.0
        self.total_prompt_tokens = 0
        self.total_completion_tokens = 0

    def clean_javadoc_for_ai(self, raw_doc):
        if not raw_doc: return ""
        lines = raw_doc.splitlines()
        clean = [line.strip().lstrip('/*').lstrip('*').strip() for line in lines 
                 if not any(tag in line for tag in ['@param', '@return', '@throws'])]
        return " ".join(clean)

    def get_analysis(self, method_name, class_name, code, visibility="public", raw_javadoc=""):
        doc_text = self.clean_javadoc_for_ai(raw_javadoc)
        
        # Check for Public specifically
        if visibility.lower() == "public":
            target_model = PublicMethodAnalysis
            role_instruction = "This is PUBLIC API method. Focus on external integration and real-world use cases. Focus on its 'API shape'—what it inputs, what it outputs, and its side effects."
        else:
            # Handles private, protected, and default (package-private)
            target_model = PrivateMethodAnalysis
            role_instruction = f"This is INTERNAL ({visibility}) helper method. Focus on how it supports class logic.Focus on its specific role within the class logic and how it supports other methods."
        
        # Enhanced System Prompt for Flow Generation
        system_msg = (
            "You are a senior Java architect. Analyze the provided code to understand its role in a larger system. "
            f"{role_instruction} Return ONLY JSON."
        )

        prompt = ChatPromptTemplate.from_messages([
            ("system", system_msg),
            ("user", "Method: {method_name}\nClass: {class_name}\nJavadoc: {javadoc}\nCode:\n{code}\n\n{format_instructions}")
        ])

        chain = prompt | self.llm | self.parser
        
        with get_openai_callback() as cb:
            try:
                result = chain.invoke({
                    "method_name": method_name, 
                    "class_name": class_name,
                    "javadoc": doc_text or "None", 
                    "code": code,
                    "format_instructions": self.parser.get_format_instructions()
                })
                
                # Add usage stats to the result so they can be saved in DB
                result['_tokens'] = cb.total_tokens
                result['_cost'] = cb.total_cost
                
                self.total_cost += cb.total_cost
                self.total_prompt_tokens += cb.prompt_tokens
                self.total_completion_tokens += cb.completion_tokens
                
                return result
            except Exception as e:
                print(f"Error analyzing {method_name}: {e}")
                return None
