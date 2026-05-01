-- Script exécuté automatiquement par postgres au premier démarrage
-- Crée la base Airflow en plus de la base analytics principale
SELECT 'CREATE DATABASE airflow OWNER ' || current_user
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec

-- Création base analytics
SELECT 'CREATE DATABASE analytics OWNER ' || current_user
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'analytics'
)\gexec

-- Schémas pour le DWH analytics
\c analytics

CREATE SCHEMA IF NOT EXISTS marts;

COMMENT ON SCHEMA marts   IS 'Modèles finaux exposés aux utilisateurs analytiques';
