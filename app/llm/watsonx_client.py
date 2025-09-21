import os
from ibm_watsonx_ai.foundation_models import Model
from ibm_watsonx_ai.metanames import GenTextParamsMetaNames as GenParams

def llm_generate(prompt: str) -> str:
    model_id = os.getenv("WX_MODEL_ID", "ibm/granite-13b-instruct-v2")
    params = {
        GenParams.MAX_NEW_TOKENS: 800,
        GenParams.TEMPERATURE: 0.2,
        GenParams.DECODING_METHOD: "greedy",
        GenParams.REPETITION_PENALTY: 1.05,
    }
    m = Model(
        model_id=model_id,
        params=params,
        credentials={"url": os.getenv("WX_URL"), "apikey": os.getenv("WX_API_KEY")},
        project_id=os.getenv("WX_PROJECT_ID")
    )
    out = m.generate_text(prompt=prompt)
    return out.get("results", [{}])[0].get("generated_text", "")
