from groq import Groq
import os
import json
import time

client = Groq(api_key=os.environ['GROQ_API_KEY'])

def label_cluster(cluster_id: int, sample_texts: list) -> dict:
    samples = '\n'.join([f'- {t}' for t in sample_texts[:10]])
    prompt = f'''
以下はECサイトのレビューコメント群です（ポルトガル語）。
これらのレビューに共通するテーマを日本語で答えてください。

レビュー一覧:
{samples}

出力形式（JSONのみ、説明不要）:
{{
  "label": "テーマ名（10文字以内）",
  "summary": "共通する内容の要約（50文字以内）"
}}
'''
    response = client.chat.completions.create(
        model='llama-3.3-70b-versatile',
        messages=[{'role': 'user', 'content': prompt}],
        temperature=0.0,
        max_tokens=200,
    )
    result = json.loads(response.choices[0].message.content)
    result['cluster_id'] = cluster_id
    time.sleep(2)
    return result

if __name__ == '__main__':
    from fetch_reviews import fetch_reviews
    from embed import generate_embeddings
    from cluster import find_optimal_k, cluster_reviews

    df = fetch_reviews(limit=300)
    embeddings = generate_embeddings(df['review_text'].tolist())
    k = find_optimal_k(embeddings, range(5, 11))
    df['cluster_id'] = cluster_reviews(embeddings, k)

    for cid in range(k):
        samples = df[df['cluster_id'] == cid]['review_text'].tolist()
        result = label_cluster(cid, samples)
        print(f"Cluster {cid}: {result['label']} → {result['summary']}")
