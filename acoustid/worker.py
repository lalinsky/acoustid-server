
# Copyright (C) 2023 Lukas Lalinsky
# Distributed under the MIT license, see the LICENSE file for details.

import logging
import time

from acoustid.script import Script
from acoustid.tasks import dequeue_task

from acoustid.scripts.update_stats import run_update_stats
from acoustid.scripts.update_lookup_stats import run_update_lookup_stats, run_update_all_lookup_stats
from acoustid.scripts.update_user_agent_stats import run_update_user_agent_stats, run_update_all_user_agent_stats

logger = logging.getLogger(__name__)


TASKS = {
    'update_stats': run_update_stats,
    'update_lookup_stats': run_update_lookup_stats,
    'update_all_lookup_stats': run_update_all_lookup_stats,
    'update_user_agent_stats': run_update_user_agent_stats,
    'update_all_user_agent_stats': run_update_all_user_agent_stats,
}


def run_worker(script: Script) -> None:
    logger.info('Starting worker')
    while True:
        try:
            name, kwargs = dequeue_task(script.get_redis(), timeout=10.0)
        except TimeoutError:
            logger.debug('No tasks to run')
            time.sleep(1.0)
            continue

        func = TASKS.get(name)
        if func is None:
            logger.error('Unknown task: %s', name)
            continue

        logger.info('Running task %s(%s)', name, kwargs)

        try:
            func(script, **kwargs)  # type: ignore
        except Exception:
            logger.exception('Error running task: %s', name)
            continue

        logger.debug('Finished task %s(%s)', name, kwargs)