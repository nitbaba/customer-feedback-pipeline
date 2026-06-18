import json
import time

def call_mock_llm(review_text: str) -> str:
    """
    Simulates LLM api response categorizing a support ticket or customer product review
    """

    #network latency
    time.sleep(0.1)

    text_lower = review_text.lower()

    #triage rules simulating smart parsing
    if "battery" in text_lower or "dies" in text_lower or "broke" in text_lower:
        sentiment = "Negative"
        urgency_score = 5
        summary = "Hardware failure flag: Product reporting physical breakdown."
    elif "confusing" in text_lower or "delayed" in text_lower:
        sentiment = "Negative"
        urgency_score = 3
        summary = "Operational friction flag: Experiencing setup or logistic delays."
    else:
        sentiment = "Positive"
        urgency_score = 1
        summary = "User satisfaction loop: General positive sentiment captured."

    result = {
        "sentiment": sentiment,
        "urgency_score": urgency_score,
        "summary": summary
    }

    return json.dumps(result)