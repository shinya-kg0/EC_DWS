from sentence_transformers import SentenceTransformer
import numpy as np

MODEL_NAME = 'paraphrase-multilingual-MiniLM-L12-v2'

def generate_embeddings(texts: list) -> np.ndarray:
    model = SentenceTransformer(MODEL_NAME)
    embeddings = model.encode(
        texts,
        batch_size=64,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    return embeddings

if __name__ == '__main__':
    sample = ['Produto excelente!', 'Não recebi o produto ainda', 'Entrega rápida']
    emb = generate_embeddings(sample)
    print(f'Shape: {emb.shape}')
    print(f'First vector (first 5 dims): {emb[0][:5]}')
