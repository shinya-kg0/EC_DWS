import sys
import os
sys.path.append(os.path.dirname(__file__))

from fetch_reviews import fetch_reviews, fetch_review_count
from embed import generate_embeddings, reduce_dimensions
from cluster import find_optimal_k, cluster_reviews
from label import label_cluster
from write_back import write_clusters_to_snowflake


def run(limit: int = 1000):

    print('--- Step 0: Checking review count ---')
    counts = fetch_review_count()

    if counts['unprocessed'] == 0:
        print('No unprocessed reviews found. Pipeline skipped.')
        return

    print(
        f"Unprocessed: {int(counts['unprocessed'])} / "
        f"Total: {int(counts['total'])} reviews"
    )

    print(f'--- Step 1: Fetching up to {limit} unprocessed reviews ---')
    df = fetch_reviews(limit=limit)

    if df.empty:
        print('No reviews fetched. Pipeline skipped.')
        return

    print(f'Fetched {len(df)} reviews')

    print('--- Step 2: Generating embeddings ---')
    embeddings = generate_embeddings(df['review_text'].tolist())

    print('--- Step 2.5: Reducing dimensions with t-SNE ---')
    coords = reduce_dimensions(embeddings)
    df['tsne_x'] = coords[:, 0]
    df['tsne_y'] = coords[:, 1]

    print('--- Step 3: Clustering ---')
    n = len(df)
    k_max = min(15, max(3, n // 20))
    k = find_optimal_k(embeddings, range(3, k_max + 1))
    df['cluster_id'] = cluster_reviews(embeddings, k)
    print(f'Optimal k = {k}')

    print('--- Step 4: Labeling clusters with Groq ---')
    labels = {}
    for cid in range(k):
        samples = df[df['cluster_id'] == cid]['review_text'].tolist()
        result = label_cluster(cid, samples)
        labels[cid] = result
        print(f'  Cluster {cid}: {result["label"]}')

    df['cluster_label']   = df['cluster_id'].map(lambda x: labels[x]['label'])
    df['cluster_summary'] = df['cluster_id'].map(lambda x: labels[x]['summary'])

    print('--- Step 5: Writing to Snowflake (incremental merge) ---')
    write_clusters_to_snowflake(df)

    print('--- Step 6: Final count check ---')
    fetch_review_count()

    print('Pipeline complete!')


if __name__ == '__main__':
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
    run(limit=limit)