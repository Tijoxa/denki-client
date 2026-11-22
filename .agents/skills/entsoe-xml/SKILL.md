---
name: entsoe-xml
description: Analyse les documents XML et ZIP de la Transparency Platform ENTSO-E et guide l’ajout ou la correction des requêtes, du parseur Rust et des projections Polars dans denki-client.
---

# XML de la Transparency Platform ENTSO‑E

## Trouver le contrat de la requête

- Partir du [catalogue de `plan.md`](../../../plan.md#catalogue-des-requêtes) pour trouver la fiche Postman, les params HTTP et les limites de fenêtre ou de pagination.
- Lire [l’index des exemples Postman](references/postman-xml.md) pour retrouver les corps de requête, les réponses enregistrées, le format des archives et les cas où un exemple présente seulement un extrait.
- Lire [le corpus officiel et les XSD](references/corpus-xml.md) pour comparer les versions d’un même document ou trouver une structure absente de Postman. Choisir le XSD par racine et URI d’espace de noms. Lire [les cas de parsing](references/parsing.md) pour les structures récurrentes et les tests de régression.

## Modifier le parseur ou ajouter une requête

1. Identifier le conteneur à partir des octets. Déplier le ZIP et ses membres ZIP, puis lire chaque XML avec le parseur Rust. Dans Postman, une ligne entre crochets peut désigner un membre d’archive. Utiliser le contenu XML du membre pour un test de parsing et de vrais octets ZIP pour un test du transport.
2. Identifier le document par sa racine et son espace de noms. Extraire les valeurs par chemin relatif à leur document, série, période ou point. Conserver les éléments répétés, les attributs et les champs absents. Les noms XML qui contiennent des points forment des noms locaux à part entière.
3. Vérifier le grain et le schéma de la projection Polars. Pour une série A03, conserver les bornes des blocs et les lacunes documentées. Pour un référentiel, une indisponibilité ou un OMI, utiliser la structure du document pour identifier les lignes. Traiter les `Reason` d’un acquittement avant de conclure à une absence de données.
4. Tester le contrat HTTP avec `httpx2.MockTransport` et un exemple versionné ou un XML de test fidèle au schéma. Vérifier l’orientation des zones, la pagination, les types et les valeurs dans les lignes retournées. Suivre le [skill de revue des tests](../review-tests/SKILL.md) pour choisir les suites pertinentes.
