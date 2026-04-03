from logging_utils import get_logger
import ingest_arxiv
import ingest_github

logger = get_logger(__name__)

def main() -> None:
    logger.info("Starting data ingestion...")
    
    try:
        ingest_arxiv.run()
    except Exception as e:
        logger.exception("arXiv ingestion failed: %s", e)

    try:
        ingest_github.run()
    except Exception as e:
        logger.exception("GitHub ingestion failed: %s", e)
    
    logger.info("Data ingestion completed.")

if __name__ == "__main__":
    main()