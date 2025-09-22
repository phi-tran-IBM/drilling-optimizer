from typing import List, Tuple
from sklearn.feature_extraction.text import TfidfVectorizer
import re

def suggest_topics(text: str, top_k: int = 6) -> List[str]:
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        raise ValueError("Empty text for topic suggestion")
    vec = TfidfVectorizer(
        stop_words="english",
        ngram_range=(1,2),
        max_df=0.9,
        min_df=1,
        lowercase=True,
        token_pattern=r"(?u)\b[a-zA-Z][a-zA-Z0-9\-]{2,}\b"
    )
    X = vec.fit_transform([text])
    vocab = vec.get_feature_names_out()
    weights = X.toarray()[0]
    pairs = sorted(zip(vocab, weights), key=lambda x: x[1], reverse=True)
    topics = []
    for term, _ in pairs:
        if any(ch.isdigit() for ch in term): continue
        if len(term) < 4: continue
        topics.append(term.replace('-', ' '))
        if len(topics) >= top_k: break
    return topics
