---
name: review-tests
description: Relit les tests ajoutés ou modifiés et choisit les vérifications Cargo et pytest pertinentes pour un changement Python ou Rust dans denki-client.
---

# Revoir et exécuter les tests pertinents

Examiner le diff et la couverture existante avant de lancer les tests. Inclure les tests Rust intégrés aux modules (`#[cfg(test)]`). Parmi les tests ajoutés pendant la tâche, retirer ceux qui doublonnent une couverture existante, vérifient seulement le comportement d’un mock ou n’ont aucune assertion significative sur le code. Préserver les tests préexistants, les modifications de l’utilisateur, la couverture demandée et les tests qui détectent une régression distincte.

Choisir les suites selon les comportements touchés et commencer par les cas ciblés. Sauter les suites pour la documentation, le formatage et les réglages du hook Codex. Exécuter les suites explicitement demandées par l’utilisateur.

- Pour un changement du parseur ou du comportement Rust couvert par des tests Rust, utiliser `cargo test` avec un filtre si le périmètre est localisé.
- Pour un changement du client Python, de son contrat HTTP, d’une projection Polars ou d’un comportement Rust exposé à Python, utiliser `uv run pytest -m "not live"` en ciblant les fichiers concernés quand c’est possible. Après un changement de l’extension Rust, ajouter `--reinstall-package denki-client` à `uv run` pour reconstruire l’extension avant pytest.
- Exécuter les deux suites si le changement touche à la fois le comportement Rust et celui exposé à Python. Réserver les tests `live` aux vérifications qui nécessitent réellement l’API ENTSO‑E et pour lesquelles les identifiants sont disponibles.
