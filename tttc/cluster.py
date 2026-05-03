from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import numpy as np

def find_optimal_k(
    embeddings: np.ndarray,
    k_range: range = range(5, 16)
) -> int:
    scores = {}
    for k in k_range:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(embeddings)
        scores[k] = silhouette_score(embeddings, labels, sample_size=300)
        print(f'  k={k}: silhouette={scores[k]:.4f}')
    best_k = max(scores, key=scores.get)
    print(f'Best k: {best_k}')
    return best_k

def cluster_reviews(embeddings: np.ndarray, k: int) -> np.ndarray:
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    return km.fit_predict(embeddings)

if __name__ == '__main__':
    from fetch_reviews import fetch_reviews
    from embed import generate_embeddings

    df = fetch_reviews(limit=300)
    embeddings = generate_embeddings(df['review_text'].tolist())
    k = find_optimal_k(embeddings, range(5, 11))
    df['cluster_id'] = cluster_reviews(embeddings, k)
    print(df['cluster_id'].value_counts())