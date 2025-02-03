from couchbase_columnar.cluster import Cluster
from couchbase_columnar.credential import Credential
from typing import List, Dict
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import threading

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_cluster(connstr: str, username: str, password: str) -> Cluster:
    """Create cluster connection"""
    cred = Credential.from_username_and_password(username, password)
    return Cluster.create_instance(connstr, cred)


def get_collections(cluster: Cluster) -> List[str]:
    """Get all collections with retry logic"""
    query = """
        SELECT VALUE d.DatabaseName || '.' || d.DataverseName || '.' || d.DatasetName
        FROM System.Metadata.`Dataset` d
        WHERE d.DataverseName <> "Metadata";
    """

    max_retries = 3
    for attempt in range(max_retries):
        try:
            res = cluster.execute_query(query)
            return res.get_all_rows()
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying...")
            time.sleep(2 ** attempt)


def drop_collection(cluster: Cluster, collection: str) -> tuple[bool, str]:
    """
    Drop a single collection
    Returns: (success: bool, status: str)
    """
    try:
        if collection == 'Default.Default.feast_driver_hourly_stats':
            return False, "error"
        drop_query = f'DROP COLLECTION {collection};'
        cluster.execute_query(drop_query)
        return True, "success"
    except Exception as e:
        error_str = str(e)
        if '"code":24025' in error_str:  # Collection not found
            return False, "not_found"
        else:
            return False, "error"


class ProgressTracker:
    def __init__(self, total: int):
        self.lock = threading.Lock()
        self.stats = {
            'successful': 0,
            'not_found': 0,
            'failed': 0,
            'processed': 0,
            'total': total
        }

    def update(self, success: bool, status: str, collection: str):
        with self.lock:
            if success:
                self.stats['successful'] += 1
                logger.info(f'Successfully dropped collection: {collection}')
            elif status == "not_found":
                self.stats['not_found'] += 1
                logger.warning(f'Collection not found (may be already dropped): {collection}')
            else:
                self.stats['failed'] += 1
                logger.error(f'Error dropping collection {collection}')

            self.stats['processed'] += 1
            logger.info(
                f"Progress: {self.stats['processed']}/{self.stats['total']} collections processed "
                f"({self.stats['successful']} successful, {self.stats['not_found']} not found, "
                f"{self.stats['failed']} failed)"
            )


def process_collection(cluster: Cluster, collection: str, tracker: ProgressTracker):
    """Process a single collection and update progress"""
    success, status = drop_collection(cluster, collection)
    tracker.update(success, status, collection)


def main() -> None:
    connstr = 'couchbases://cb.9d1ccd1-9osjioll.cloud.couchbase.com'
    username = 'username'
    password = 'Password!123'

    # DONT DROP feast_driver_hourly_stats
    # connstr = 'couchbases://cb.zldvu6s2qoj8zuoc.cloud.couchbase.com'
    # username = 'username'
    # password = 'Password!123'

    try:
        cluster = create_cluster(connstr, username, password)

        logger.info("Fetching collections...")
        collections = get_collections(cluster)
        total_collections = len(collections)
        logger.info(f'Found {total_collections} collections')

        # Initialize progress tracker
        tracker = ProgressTracker(total_collections)

        # Process collections in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(process_collection, cluster, collection, tracker)
                for collection in collections
            ]
            # Wait for all tasks to complete
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Unexpected error in worker thread: {e}")

        # Final summary
        logger.info("\nFinal results:")
        logger.info(f"Successfully dropped: {tracker.stats['successful']}")
        logger.info(f"Not found/already dropped: {tracker.stats['not_found']}")
        logger.info(f"Failed: {tracker.stats['failed']}")

    except Exception as e:
        logger.error(f"Critical error: {e}")
        raise


if __name__ == '__main__':
    main()
