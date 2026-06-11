use serde::{Deserialize, Serialize};

// ── Embedding types ─────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HybridSearchResult {
    pub category_name: String,
    pub key_path: String,
    pub value_text: String,
    pub snippet: Option<String>,
    pub keyword_score: f64,
    pub semantic_score: f64,
    pub combined_score: f64,
}

// ── Simple TF-IDF embedder for local-only semantic search ───────
// No external model required. Uses term frequency vectors
// normalized to unit length for cosine similarity.

pub struct LocalEmbedder {
    vocabulary: Vec<String>,
    idf_weights: Vec<f32>,
}

impl Default for LocalEmbedder {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalEmbedder {
    pub fn new() -> Self {
        Self {
            vocabulary: Vec::new(),
            idf_weights: Vec::new(),
        }
    }

    /// Build vocabulary from all indexed text entries.
    pub fn build_vocabulary(&mut self, texts: &[String]) {
        let mut doc_freq: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
        let total_docs = texts.len().max(1) as f32;

        for text in texts {
            let tokens = tokenize(text);
            let unique_tokens: std::collections::HashSet<&str> =
                tokens.iter().map(|s| s.as_str()).collect();
            for token in unique_tokens {
                *doc_freq.entry(token.to_string()).or_insert(0) += 1;
            }
        }

        let max_df = total_docs * 0.8;
        let mut vocab: Vec<(String, f32)> = doc_freq
            .into_iter()
            .filter(|(_, df)| *df as f32 <= max_df)
            .map(|(term, df)| {
                let idf = (total_docs / (df as f32 + 1.0)).ln() + 1.0;
                (term, idf)
            })
            .collect();
        vocab.sort_by(|a, b| a.0.cmp(&b.0));

        self.vocabulary = vocab.iter().map(|(t, _)| t.clone()).collect();
        self.idf_weights = vocab.iter().map(|(_, idf)| *idf).collect();

        tracing::info!(
            "Built vocabulary: {} terms from {} docs",
            self.vocabulary.len(),
            texts.len()
        );
    }

    /// Embed a text string into a TF-IDF vector.
    pub fn embed(&self, text: &str) -> Vec<f32> {
        let tokens = tokenize(text);
        let mut tf = vec![0.0f32; self.vocabulary.len()];

        for token in &tokens {
            if let Ok(idx) = self.vocabulary.binary_search(token) {
                tf[idx] += 1.0;
            }
        }

        let mut norm = 0.0f32;
        for (i, tf_val) in tf.iter().enumerate() {
            let weighted = tf_val * self.idf_weights.get(i).copied().unwrap_or(1.0);
            norm += weighted * weighted;
        }
        norm = norm.sqrt().max(1e-8);
        for (i, tf_val) in tf.iter_mut().enumerate() {
            *tf_val *= self.idf_weights.get(i).copied().unwrap_or(1.0);
            *tf_val /= norm;
        }

        tf
    }

    /// Compute cosine similarity between two embeddings.
    pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() || a.is_empty() {
            return 0.0;
        }
        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm_a < 1e-8 || norm_b < 1e-8 {
            return 0.0;
        }
        dot / (norm_a * norm_b)
    }
}

fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|s| s.len() > 1)
        .map(|s| s.to_string())
        .collect()
}

// ── Hybrid search (keyword FTS + semantic) ──────────────────────

pub fn hybrid_search(
    keyword_results: Vec<(String, String, String, Option<String>)>,
    all_entries: Vec<(String, String, String)>,
    embedder: &LocalEmbedder,
    query: &str,
    limit: u32,
    semantic_weight: f64,
) -> Vec<HybridSearchResult> {
    let query_embedding = embedder.embed(query);

    let keyword_set: std::collections::HashSet<(String, String)> = keyword_results
        .iter()
        .map(|(cat, path, _, _)| (cat.clone(), path.clone()))
        .collect();

    let mut scored: Vec<HybridSearchResult> = Vec::new();

    for (cat, path, text) in &all_entries {
        let entry_embedding = embedder.embed(text);
        let sim = LocalEmbedder::cosine_similarity(&query_embedding, &entry_embedding) as f64;

        let kw_score = if keyword_set.contains(&(cat.clone(), path.clone())) {
            1.0
        } else {
            0.0
        };

        let combined = semantic_weight * sim + (1.0 - semantic_weight) * kw_score;

        if combined > 0.1 {
            let snippet = keyword_results
                .iter()
                .find(|(c, p, _, _)| c == cat && p == path)
                .and_then(|(_, _, _, s)| s.clone());

            scored.push(HybridSearchResult {
                category_name: cat.clone(),
                key_path: path.clone(),
                value_text: text.clone(),
                snippet,
                keyword_score: kw_score,
                semantic_score: sim,
                combined_score: combined,
            });
        }
    }

    scored.sort_by(|a, b| {
        b.combined_score
            .partial_cmp(&a.combined_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    scored.truncate(limit as usize);

    scored
}
