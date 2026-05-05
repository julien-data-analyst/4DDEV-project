"""
collecte_taxi_trips.py
======================
DAG Airflow – Collecte BATCH des trajets de taxis jaunes NYC.

Source  : https://d37ci6vzurychx.cloudfront.net/trip-data/
Format  : Parquet
Cible   : Minio → bucket "raw-taxi" → yellow_tripdata/YYYY/MM/
Plage   : 2026-01 → mois courant

Logique :
  - On interroge Minio pour savoir quels fichiers sont déjà présents.
  - On ne télécharge que les fichiers manquants (idempotent).
  - Chaque fichier est streamé par blocs pour limiter la mémoire.
  - Un fichier _metadata.json est écrit à côté de chaque Parquet.
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import date, datetime
import time
import boto3
import requests
from botocore.client import Config
from dateutil.relativedelta import relativedelta

# ─────────────────────────────────────────────────────────────────────────────
#  Configuration (variables d'env injectées par Airflow / docker-compose)
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
BUCKET = "raw-taxi"
PREFIX = "yellow_tripdata"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("DATALAKE_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("DATALAKE_PASSWORD", "minio_password_change_me")

FIRST_YEAR = 2009 
FIRST_MONTH = 1
CHUNK_SIZE = 8 * 1024 * 1024  # 8 Mo par chunk (streaming HTTP)
REQUEST_TIMEOUT = 60           # secondes

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
#  Client Minio (compatible S3)
# ─────────────────────────────────────────────────────────────────────────────

def get_s3_client() -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Utilitaires
# ─────────────────────────────────────────────────────────────────────────────

from datetime import date
from dateutil.relativedelta import relativedelta

def iter_months_range(start, end):
    current = start
    while current <= end:
        yield current
        current += relativedelta(months=1)


def s3_key(year: int, month: int) -> str:
    return f"{PREFIX}/{year:04d}/{month:02d}/yellow_tripdata_{year:04d}-{month:02d}.parquet"


def metadata_key(year: int, month: int) -> str:
    return f"{PREFIX}/{year:04d}/{month:02d}/_metadata.json"


def file_exists_in_minio(s3: boto3.client, key: str) -> bool:
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False
    except Exception:
        return False


def download_and_upload(s3: boto3.client, url: str, bucket: str, key: str) -> int:
    """
    Télécharge un fichier avec requête HTTP et l'envoie directement dans Minio
    via l'API multipart upload. Retourne la taille totale en octets.
    """
    log.info("Téléchargement → %s", url)

    with requests.get(url, stream=True, timeout=REQUEST_TIMEOUT) as resp:
        if resp.status_code == 404:
            log.warning("Fichier non disponible (404) : %s", url)
            return 0
        resp.raise_for_status()

        content_length = int(resp.headers.get("Content-Length", 0))
        log.info("Taille annoncée : %.1f Mo", content_length / 1_048_576)

        # Multipart upload Minio
        mpu = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mpu["UploadId"]
        parts = []
        part_number = 1
        buffer = io.BytesIO()
        total_bytes = 0

        try:
            for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                buffer.write(chunk)
                total_bytes += len(chunk)

                if buffer.tell() >= CHUNK_SIZE:
                    buffer.seek(0)
                    part = s3.upload_part(
                        Bucket=bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=buffer.read(),
                    )
                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    part_number += 1
                    buffer = io.BytesIO()

            # Dernier chunk résiduel
            if buffer.tell() > 0:
                buffer.seek(0)
                part = s3.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer.read(),
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})

            s3.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                MultipartUpload={"Parts": parts},
                UploadId=upload_id,
            )
            log.info("Upload terminé : %s (%.1f Mo)", key, total_bytes / 1_048_576)
            return total_bytes

        except Exception as exc:
            log.error("Erreur upload, annulation multipart : %s", exc)
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
            raise


def write_metadata(s3: boto3.client, year: int, month: int, size_bytes: int) -> None:
    meta = {
        "source": f"{BASE_URL}/yellow_tripdata_{year:04d}-{month:02d}.parquet",
        "year": year,
        "month": month,
        "ingested_at": datetime.utcnow().isoformat() + "Z",
        "size_bytes": size_bytes,
    }
    s3.put_object(
        Bucket=BUCKET,
        Key=metadata_key(year, month),
        Body=json.dumps(meta, indent=2).encode(),
        ContentType="application/json",
    )


# ─────────────────────────────────────────────────────────────────────────────
#  Point d'entrée principal (appelé par le DAG Airflow ou en standalone)
# ─────────────────────────────────────────────────────────────────────────────

def collect_taxi_batch(
    year_from: int = FIRST_YEAR,
    month_from: int = FIRST_MONTH,
    year_to: int = None,
    month_to: int = None,
    force: bool = False,
) -> dict:
    """
    Collecte tous les fichiers Parquet manquants dans Minio.

    Args:
        year_from:  Année de début (défaut 2009).
        month_from: Mois de début (défaut 1).
        force:      Si True, re-télécharge même si déjà présent.

    Returns:
        Dictionnaire de résumé avec compteurs.
    """
    s3 = get_s3_client()
    stats = {"downloaded": 0, "skipped": 0, "errors": 0, "total_bytes": 0}

    start = date(year_from, month_from, 1)
    end = (
        date(year_to, month_to, 1)
        if year_to and month_to
        else date.today().replace(day=1)
    )

    request_count = 0
    for month_date in iter_months_range(start, end):
        year = month_date.year
        month = month_date.month

        key = s3_key(year, month)
        url = f"{BASE_URL}/yellow_tripdata_{year:04d}-{month:02d}.parquet"

        if request_count >= 5 :
            time.sleep(5)

        if not force and file_exists_in_minio(s3, key):
            stats["skipped"] += 1
            continue

        try:
            request_count += 1
            size = download_and_upload(s3, url, BUCKET, key)
            if size > 0:
                write_metadata(s3, year, month, size)
                stats["downloaded"] += 1
                stats["total_bytes"] += size
        except Exception:
            stats["errors"] += 1

    return stats





# ─────────────────────────────────────────────────────────────────────────────
#  Exécution standalone pour les tests
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = collect_taxi_batch(year_from=2026, month_from=1)  # test limité à 2026
    print("Résultat :", result)
