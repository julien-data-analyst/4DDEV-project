-- Active: 1776860365260@@127.0.0.1@5434@data_warehouse
-- Script exécuté automatiquement par postgres au premier démarrage
-- Crée la base Airflow en plus de la base analytics principale
SELECT 'CREATE DATABASE airflow OWNER ' || current_user
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec

-- Schémas pour le DWH analytics
\c data_warehouse

CREATE SCHEMA IF NOT EXISTS marts;

COMMENT ON SCHEMA marts   IS 'Modèles finaux exposés aux utilisateurs analytiques';