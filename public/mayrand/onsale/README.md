# Mayrand onsale dataset

Ce dossier contient les sorties du scraper Mayrand pour la requête `onsale`.

## Fichiers

- `data.json` : liste des produits en JSON.
- `data.csv` : mêmes données en CSV.
- `metadata.json` : métadonnées de la collecte (timestamp, nombre de produits, nombre de pages).

## Champs

- `source` : `mayrand`.
- `query` : `onsale`.
- `name` : titre du produit.
- `brand` : marque si visible.
- `sku` : code produit si visible.
- `price_sale` : prix actuel (nombre, sans `$`).
- `price_regular` : prix barré si présent, sinon `null`.
- `unit_label` : libellé d’unité (ex: "unité (150G)").
- `unit_price` : prix à l’unité (ex: 1.33 pour "1,33$/100g").
- `url` : lien vers la fiche produit (ou ancre vers la recherche si indisponible).
- `image` : URL de l’image si disponible.
- `category` : catégorie si détectée.
- `scraped_at` : timestamp ISO de l’extraction.
