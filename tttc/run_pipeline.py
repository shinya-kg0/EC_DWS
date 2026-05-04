import sys
import os
sys.path.append(os.path.dirname(__file__))

from fetch_reviews import fetch_reviews
from embed import generate_embeddings, reduce_dimensions
from cluster import find_optimal_k, cluster_reviews
from label import label_cluster
from write_back import write_clusters_to_snowflake

def run(limit: int = 300):
    # 1. データ取得
    print(f'--- Step 1: Fetching {limit} reviews ---')
    df = fetch_reviews(limit=limit)
    print(f'Fetched {len(df)} reviews')

    # 2. Embedding
    print('--- Step 2: Generating embeddings ---')
    embeddings = generate_embeddings(df['review_text'].tolist())
    print('--- Step 2.5: Reducing dimensions with t-SNE ---')
    coords = reduce_dimensions(embeddings)
    df['tsne_x'] = coords[:, 0]
    df['tsne_y'] = coords[:, 1]

    # 3. クラスタリング
    print('--- Step 3: Clustering ---')
    k = find_optimal_k(embeddings, range(5, 16))
    df['cluster_id'] = cluster_reviews(embeddings, k)

    # 4. ラベリング
    print('--- Step 4: Labeling clusters with Groq ---')
    labels = {}
    for cid in range(k):
        samples = df[df['cluster_id'] == cid]['review_text'].tolist()
        result = label_cluster(cid, samples)
        labels[cid] = result
        print(f'  Cluster {cid}: {result["label"]}')

    df['cluster_label']   = df['cluster_id'].map(lambda x: labels[x]['label'])
    df['cluster_summary'] = df['cluster_id'].map(lambda x: labels[x]['summary'])

    # 5. Snowflake書き戻し
    print('--- Step 5: Writing to Snowflake ---')
    write_clusters_to_snowflake(df)
    print('Pipeline complete!')

if __name__ == '__main__':
    run(limit=1000)
