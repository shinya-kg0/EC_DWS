from sentence_transformers import SentenceTransformer
from sklearn.manifold import TSNE
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

def reduce_dimensions(embeddings: np.ndarray) -> np.ndarray:
    tsne = TSNE(
        n_components=2,
        random_state=42,
        perplexity=30,
    )
    return tsne.fit_transform(embeddings)

if __name__ == '__main__':
    sample = ['Produto excelente!', 'Não recebi o produto ainda', 'Entrega rápida']
    emb = generate_embeddings(sample)
    print(f'Embedding shape: {emb.shape}')
    coords = reduce_dimensions(emb)
    print(f't-SNE shape: {coords.shape}')
