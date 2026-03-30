/*
===============================================================================
Quality Checks / Contrôles de qualité de la couche GOLD
===============================================================================
(English Version Below)
Objectif du script :
Ce script effectue des contrôles qualité afin de valider l’intégrité, la cohérence
et l’exactitude de la couche de référence. Ces contrôles garantissent :
- L’unicité des clés de substitution dans les tables de dimensions.
- L’intégrité référentielle entre les tables de faits et de dimensions.
- La validation des relations dans le modèle de données à des fins d’analyse.
Remarques d’utilisation :
- Examiner et résoudre toute anomalie détectée lors des contrôles.
_________________
ENGLISH VERSION
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Vérifier l'unicité de la clé client dans gold.dim_customers
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key' / Vérifier 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales' / Vérifier 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions 
-- Vérifier la connexion entre les tables de faits et dimensions dans le modèle de données
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  
