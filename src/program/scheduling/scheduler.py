"""
Scheduling subsystem for Program.

Encapsulates APScheduler setup, background jobs, and time-based orchestration
for content services and item-specific schedules.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, TypedDict

from apscheduler.events import (
    EVENT_JOB_ERROR,
    EVENT_JOB_EXECUTED,
    EVENT_JOB_MISSED,
    EVENT_JOB_SUBMITTED,
)
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from program.db import db_functions
from program.db.db import db_session, vacuum_and_analyze_index_maintenance
from program.media.item import Episode, MediaItem, Movie, Show
from program.media.state import States
from program.scheduling.models import ScheduledStatus, ScheduledTask
from program.settings import settings_manager
from program.types import Event
from program.utils import data_dir_path
from program.utils.logging import log_cleaner, logger
from program.apis.tvdb_api import SeriesRelease
from schemas.tvdb.models.series_airs_days import SeriesAirsDays

if TYPE_CHECKING:
    from program.program import Program


class ScheduledFunctionConfig(TypedDict):
    interval: int


class ProgramScheduler:
    """
    Owns the BackgroundScheduler and all scheduling concerns for Program.

    This class keeps scheduling logic out of Program and wires jobs to the
    Program instance via dependency injection.
    """

    def __init__(self, program: "Program") -> None:
        self.program = program
        self.scheduler = BackgroundScheduler()
        # APScheduler keeps no execution history, so the Runners view has no
        # way to answer "did this last run succeed?". Track it ourselves: one
        # entry per job id, overwritten each run (patch 0034).
        self.run_history: dict[str, dict] = {}
        # ...and APScheduler has no persistent jobstore here, so every restart
        # re-registered each job with next_run_time=now and re-fired it. Three
        # restarts in an hour meant three full retry_library sweeps. Keep the
        # last completed run on disk so cadence survives a restart (patch 0039).
        self._job_state: dict[str, dict] = {}
        # Only long-interval jobs are worth persisting. A 60s heartbeat cannot
        # be meaningfully "re-fired" by a restart, and writing the file every
        # minute would be 1440 pointless writes a day.
        self._persistable_jobs: set[str] = set()
        self._load_job_state()

    def start(self) -> None:
        """Create and start the background scheduler with all jobs registered."""

        self._schedule_services()
        self._schedule_functions()
        self.scheduler.add_listener(
            self._record_job_event,
            EVENT_JOB_SUBMITTED | EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
        )
        self.scheduler.start()

    def stop(self) -> None:
        """Stop the background scheduler if running."""

        if self.scheduler and self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    # ------------------------------------------------------------- job state
    # Persisted last-run times (patch 0039). APScheduler's default jobstore is
    # in-memory, so without this every restart is a fresh schedule.

    @property
    def _job_state_file(self):
        return data_dir_path / "job_state.json"

    def _load_job_state(self) -> None:
        """Read persisted last-run times; never let a bad file block startup."""

        try:
            raw = json.loads(self._job_state_file.read_text())
        except FileNotFoundError:
            return
        except Exception as exc:
            logger.warning(f"Ignoring unreadable job state file: {exc}")
            return

        if not isinstance(raw, dict):
            logger.warning("Ignoring job state file: expected an object")
            return

        for job_id, record in raw.items():
            if not isinstance(record, dict) or not record.get("finished_at"):
                continue

            self._job_state[job_id] = record

            # Seed the Runners view too, so "last run" is not blank after a
            # restart. `running` is deliberately not restored: nothing is.
            try:
                finished = datetime.fromtimestamp(float(record["finished_at"]))
            except (TypeError, ValueError, OSError):
                continue

            self.run_history.setdefault(
                job_id,
                {
                    "running": False,
                    "finished_at": finished,
                    "duration_ms": record.get("duration_ms"),
                    "ok": record.get("ok"),
                    "error": record.get("error"),
                },
            )

    PERSIST_MIN_INTERVAL_S = 300

    def _persist_job_state(self, job_id: str, entry: dict) -> None:
        """Record a COMPLETED run. Never raises - this is bookkeeping."""

        if job_id not in self._persistable_jobs:
            return

        self._job_state[job_id] = {
            "finished_at": entry["finished_at"].timestamp(),
            "duration_ms": entry.get("duration_ms"),
            "ok": entry.get("ok"),
            "error": entry.get("error"),
        }

        try:
            # Write-then-rename so a crash mid-write cannot leave a truncated
            # file that would silently reset every schedule.
            tmp = self._job_state_file.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self._job_state, indent=2))
            tmp.replace(self._job_state_file)
        except Exception as exc:
            logger.warning(f"Could not persist job state: {exc}")

    def _initial_next_run(self, job_id: str, interval_seconds: int) -> datetime:
        """First fire for a job being registered, honouring its last real run.

        Never run     -> now (first boot, or a newly added job).
        Ran recently  -> when it is actually next due, so restarts cannot
                         re-trigger it. This is the whole point of the patch.
        Overdue       -> now, staggered, so a box that was down for a week
                         still catches up instead of skipping a cycle.

        Timestamps are stored as epoch seconds, not naive local ISO: this
        container runs UTC+2 while the host is UTC, and a naive value would
        shift by two hours if that ever changed.
        """

        now = datetime.now()

        if interval_seconds >= self.PERSIST_MIN_INTERVAL_S:
            self._persistable_jobs.add(job_id)
        else:
            return now

        record = self._job_state.get(job_id) or {}
        last = record.get("finished_at")

        if not last:
            return now

        try:
            due = datetime.fromtimestamp(float(last)) + timedelta(seconds=interval_seconds)
        except (TypeError, ValueError, OSError):
            return now

        if due > now:
            return due

        # Overdue. Spread the catch-up so a long outage does not fire
        # retry_library, the content polls and the vacuum in the same instant.
        self._overdue_count = getattr(self, "_overdue_count", 0) + 1
        return now + timedelta(seconds=min(self._overdue_count - 1, 6) * 10)

    # ---------------------------------------------------------------- runners
    # Introspection for the frontend Runners tab (patch 0034).

    @staticmethod
    def _event_run_key(event):
        """The scheduled fire time this event belongs to.

        APScheduler names it differently per event class: JobSubmissionEvent
        carries `scheduled_run_times` (a LIST, since a coalesced job can cover
        several missed fires) while JobExecutionEvent carries the singular
        `scheduled_run_time`. Reading only the singular form yields None for
        every submit event, so the two halves of a run never match up.
        """

        run_time = getattr(event, "scheduled_run_time", None)
        if run_time is not None:
            return run_time

        times = getattr(event, "scheduled_run_times", None)
        return times[0] if times else None

    def _record_job_event(self, event) -> None:
        """Track start/finish/outcome per job id for the Runners view.

        Events are NOT delivered in causal order: a job short enough to finish
        before the scheduler thread dispatches its EVENT_JOB_SUBMITTED will
        deliver the terminal event first. Handling them naively left `running`
        stuck True until the next fire - visible as "Mdblist content poll" that
        looked permanently running on a 24h interval (patch 0035). So key each
        run by its scheduled_run_time and ignore a SUBMITTED for a run that has
        already terminated.
        """

        entry = self.run_history.setdefault(event.job_id, {})
        run_key = self._event_run_key(event)

        if event.code == EVENT_JOB_SUBMITTED:
            if run_key is not None and entry.get("finished_run") == run_key:
                return  # terminal event for this run already arrived

            entry["started_at"] = datetime.now()
            entry["started_run"] = run_key
            entry["running"] = True
            return

        entry["running"] = False
        entry["finished_at"] = datetime.now()
        entry["finished_run"] = run_key

        # Only trust the duration when both halves describe the same run.
        started = entry.get("started_at")
        if started and entry.get("started_run") == run_key:
            entry["duration_ms"] = int(
                (entry["finished_at"] - started).total_seconds() * 1000
            )
        else:
            entry["duration_ms"] = None

        if event.code == EVENT_JOB_ERROR:
            entry["ok"] = False
            entry["error"] = str(getattr(event, "exception", "") or "error")
        elif event.code == EVENT_JOB_MISSED:
            entry["ok"] = False
            entry["error"] = "missed (misfire grace exceeded)"
        else:
            entry["ok"] = True
            entry["error"] = None

        # Persist EXECUTED and ERROR only. A MISSED job never ran, so recording
        # it would push the next fire out without the work having happened
        # (patch 0039).
        if event.code != EVENT_JOB_MISSED:
            self._persist_job_state(event.job_id, entry)

    @staticmethod
    def _runner_label(job_id: str) -> str:
        """Human label for a job id, e.g. _process_scheduled_tasks -> Process scheduled tasks."""

        name = job_id
        if name.endswith("_update"):
            return f"{name[: -len('_update')]} content poll"
        if name.endswith("_update_once"):
            return f"{name[: -len('_update_once')]} content poll (webhook)"
        return name.lstrip("_").replace("_", " ").capitalize()

    def runners(self) -> list[dict]:
        """Snapshot every registered periodic job, with cadence and last run."""

        if not self.scheduler:
            return []

        out = []
        for job in self.scheduler.get_jobs():
            interval = None
            trigger = getattr(job, "trigger", None)
            job_interval = getattr(trigger, "interval", None)
            if job_interval is not None:
                interval = int(job_interval.total_seconds())

            hist = self.run_history.get(job.id, {})

            def _iso(value):
                return value.isoformat() if value else None

            out.append(
                {
                    "id": job.id,
                    "label": self._runner_label(job.id),
                    "interval_seconds": interval,
                    "trigger": str(trigger) if trigger else None,
                    "next_run": _iso(getattr(job, "next_run_time", None)),
                    "running": bool(hist.get("running")),
                    "last_started": _iso(hist.get("started_at")),
                    "last_finished": _iso(hist.get("finished_at")),
                    "last_duration_ms": hist.get("duration_ms"),
                    "last_ok": hist.get("ok"),
                    "last_error": hist.get("error"),
                }
            )

        out.sort(key=lambda r: (r["next_run"] is None, r["next_run"] or ""))
        return out

    def _schedule_functions(self) -> None:
        """Register internal periodic functions and maintenance tasks."""

        assert self.scheduler is not None

        scheduled_functions = dict[Callable[..., None], ScheduledFunctionConfig](
            {
                vacuum_and_analyze_index_maintenance: {"interval": 60 * 60 * 24},
            }
        )

        # Add retry_library if enabled (interval > 0)
        retry_interval = settings_manager.settings.retry_interval

        if retry_interval > 0:
            scheduled_functions[self._retry_library] = {"interval": retry_interval}

        # Add log_cleaner if enabled (interval > 0)
        clean_interval = settings_manager.settings.logging.clean_interval

        if clean_interval > 0:
            scheduled_functions[log_cleaner] = {"interval": clean_interval}

        # Add scheduler processing and monitoring
        scheduled_functions[self._process_scheduled_tasks] = {"interval": 60}
        scheduled_functions[self._monitor_ongoing_schedules] = {"interval": 15 * 60}

        for func, config in scheduled_functions.items():
            job_id = f"{func.__name__}"
            self.scheduler.add_job(
                func,
                "interval",
                seconds=config["interval"],
                args=config.get("args"),
                id=job_id,
                max_instances=config.get("max_instances", 1),
                replace_existing=True,
                next_run_time=self._initial_next_run(job_id, config["interval"]),
                misfire_grace_time=30,
            )

            logger.debug(
                f"Scheduled {func.__name__} to run every {config['interval']} seconds."
            )

    def _schedule_services(self) -> None:
        """Schedule each content service based on its update interval or webhook mode."""

        assert self.scheduler
        assert self.program.services

        for service_instance in self.program.services.content_services:
            service_name = service_instance.__class__.__name__

            # If the service supports webhooks and webhook mode is enabled, run once now
            use_webhook = getattr(
                getattr(service_instance, "settings", object()), "use_webhook", False
            )

            if use_webhook:
                self.scheduler.add_job(
                    self.program.em.submit_job,
                    "date",
                    run_date=datetime.now(),
                    args=[service_instance, self.program],
                    id=f"{service_name}_update_once",
                    replace_existing=True,
                    misfire_grace_time=30,
                )

                logger.debug(
                    f"Scheduled {service_name} to run once (webhook mode enabled)."
                )

                continue

            update_interval = getattr(
                service_instance.settings, "update_interval", False
            )

            if not update_interval:
                continue

            job_id = f"{service_name}_update"
            self.scheduler.add_job(
                self.program.em.submit_job,
                "interval",
                seconds=update_interval,
                args=[service_instance, self.program],
                id=job_id,
                max_instances=1,
                replace_existing=True,
                next_run_time=self._initial_next_run(job_id, update_interval),
                coalesce=False,
            )

            logger.debug(
                f"Scheduled {service_name} to run every {update_interval} seconds."
            )

    def _retry_library(self) -> None:
        """Retry items that failed to download by emitting events into the EM."""

        item_ids = db_functions.retry_library()

        for item_id in item_ids:
            self.program.em.add_event(Event(emitted_by="RetryLibrary", item_id=item_id))

        if item_ids:
            logger.log(
                "PROGRAM",
                f"Successfully retried {len(item_ids)} incomplete items",
            )
        else:
            logger.log("NOT_FOUND", "No items required retrying")

    def _get_pending_scheduled_tasks(self, session: Session) -> Sequence[ScheduledTask]:
        """Return all pending scheduled tasks."""

        try:
            return (
                session.execute(
                    select(ScheduledTask)
                    .where(ScheduledTask.status == ScheduledStatus.Pending)
                    .where(ScheduledTask.scheduled_for <= datetime.now())
                    .order_by(ScheduledTask.scheduled_for.asc())
                )
                .unique()
                .scalars()
                .all()
            )
        except SQLAlchemyError as e:
            logger.error(f"Scheduler DB error: {e}")
            return []

    def _process_scheduled_tasks(self) -> None:
        """
        Process due scheduled tasks by delegating to focused helpers.

        Responsibilities split into:
        - fetching due tasks;
        - loading/merging the target item for a task;
        - handling reindex vs. release tasks;
        - updating task status with consistent error handling.
        """
        try:
            with db_session() as session:
                now = datetime.now()
                due_tasks = self._get_pending_scheduled_tasks(session)
                if not due_tasks:
                    return

                for task in due_tasks:
                    self._process_single_scheduled_task(session, task, now)
        except SQLAlchemyError as e:
            logger.error(f"Scheduler DB error: {e}")

    def _process_single_scheduled_task(
        self,
        session: Session,
        task: ScheduledTask,
        now: datetime,
    ) -> None:
        """
        Process a single ScheduledTask instance.

        Args:
            session: Active SQLAlchemy session.
            task: The scheduled task to process.
            now: Current timestamp used for status updates.
        """
        try:
            item = self._load_item_for_task(session, task)

            if not item:
                # ScheduledTask.item_id has NO foreign key to MediaItem, so
                # removing an item leaves its tasks behind. Marking them Failed
                # kept them forever and they were pure noise: on 2026-08-24 all
                # 226 "failed" tasks were orphans of deleted items, nothing had
                # actually gone wrong. Delete instead - there is no item left to
                # retry, and the row can never become valid again (patch 0035).
                session.delete(task)
                session.commit()

                logger.debug(
                    f"ScheduledTask {task.id} item {task.item_id} no longer exists; task removed"
                )

                return

            if task.task_type in ("reindex_show", "reindex", "reindex_movie"):
                self._run_reindex_for_item(session, item)
            else:
                self._enqueue_item_if_needed(session, item)

            self._mark_task_status(
                session,
                task,
                ScheduledStatus.Completed,
                datetime.now(),
            )
        except Exception as e:
            session.rollback()
            self._mark_task_status(
                session, task, ScheduledStatus.Failed, datetime.now()
            )
            logger.exception(f"Failed processing ScheduledTask {task.id}: {e}")

    def _load_item_for_task(self, session: Session, task: ScheduledTask):
        """
        Load and merge the MediaItem for a scheduled task.

        Returns:
            The merged item or None if missing.
        """

        item = db_functions.get_item_by_id(task.item_id, session=session)

        if not item:
            return None

        return session.merge(item)

    def _run_reindex_for_item(self, session: Session, item: MediaItem) -> None:
        """Run indexer service for an item if available and persist updates."""

        assert self.program.services, "Services not initialized in Program"

        indexer_service = self.program.services.indexer

        updated = next(indexer_service.run(item, log_msg=False), None)

        if updated:
            # Use no_autoflush so SQLAlchemy doesn't try to flush the transient
            # Season/Episode objects the indexer just created before the merge
            # has completed.
            with session.no_autoflush:
                merged = session.merge(updated.media_items[0])

            # SQLAlchemy 2.0 does NOT auto-cascade a transient child appended to
            # a persistent parent's collection, so the newly-aired Season/Episode
            # objects the TVDB indexer created via show.add_season() /
            # season.add_episode() are silently dropped on flush ("Object of type
            # <Season> not in session, add operation along 'Show.seasons' will
            # not proceed"). Without this the scheduled reindex logs "Reindexed
            # X from scheduler" every day yet never actually stores a newly-aired
            # season — the automatic counterpart of the bug patches 0031/0032
            # fixed on the /scrape/auto and /items/reindex routes, and the reason
            # a continuing show only ever got its new season when a user manually
            # requested it (patch 0033).

            # Membership must be sampled for the WHOLE subtree BEFORE anything
            # is added. Season.episodes is cascade="all, delete-orphan", so
            # session.add(season) cascades save-update to its episodes and any
            # episode inspected afterwards already reads as in-session. Patch
            # 0033 interleaved the check with the add, so `added` only ever
            # counted new SEASONS: Futurama S11 persisted 1 season + 10
            # episodes on 2026-08-24 and reported "1". That is not cosmetic —
            # a reindex that adds episodes to a season which already exists
            # (the episode-less season shell patch 0032 heals, or an episode
            # row deleted per patch 0036) counted 0 and therefore never
            # enqueued anything at all (patch 0040).
            new_children = list[MediaItem]()

            if isinstance(merged, Show):
                for season in merged.seasons:
                    if season not in session:
                        new_children.append(season)

                    for episode in season.episodes:
                        if episode not in session:
                            new_children.append(episode)

                for season in merged.seasons:
                    session.add(season)

                    for episode in season.episodes:
                        session.add(episode)

            added = len(new_children)

            # Episodes that had already aired by the time they were discovered
            # are the ones nothing else will ever kick: the monitor only
            # schedules an episode_release task for a FUTURE air date.
            aired_episodes = [
                child
                for child in new_children
                if isinstance(child, Episode)
                and child.last_state != States.Completed
                and child.is_released
            ]

            session.commit()

            logger.info(f"Reindexed {item.log_string} from scheduler")

            # A season discovered after its episodes already aired has no
            # upcoming-release task to enqueue it, so those episodes would sit
            # Indexed indefinitely. Nudge the pipeline, but only when the
            # reindex actually persisted something new.
            if added:
                logger.info(
                    f"Reindex persisted {added} new season/episode rows for "
                    f"{merged.log_string}; enqueueing"
                )

                # Enqueue the aired episodes THEMSELVES, not the show. A show
                # event cannot reach them: state_transition deliberately keeps
                # a Season as one unit so a season pack can be matched, and
                # only decomposes to per-episode scraping after
                # SEASON_PACK_FALLBACK_THRESHOLD=3 failed season scrapes
                # (patch 0018). A season discovered mid-air has no pack, so the
                # single season-level attempt a show event buys fails and the
                # already-aired episodes stay Indexed until a human requests
                # the season — exactly what happened to Futurama S11E01-E04
                # (enqueued 06:00:22, one failed "Futurama S11" scrape at
                # 06:00:33, still Indexed two hours later). Asking per episode
                # loses nothing: scrapers/shared.py matches a pack-shaped
                # torrent (no episodes, right season number) against an
                # Episode too.
                for episode in aired_episodes:
                    self.program.em.add_event(
                        Event(emitted_by="Scheduler", item_id=episode.id)
                    )

                if not aired_episodes:
                    self.program.em.add_event(
                        Event(emitted_by="Scheduler", item_id=merged.id)
                    )

    def _enqueue_item_if_needed(self, session: Session, item: MediaItem) -> None:
        """Refresh state and enqueue item to the event manager if not completed."""

        was_completed = item.last_state == States.Completed
        item.store_state()
        session.commit()

        if not was_completed:
            self.program.em.add_event(Event(emitted_by="Scheduler", item_id=item.id))
            logger.info(f"Enqueued {item.log_string} from scheduler")

    def _mark_task_status(
        self,
        session: Session,
        task: ScheduledTask,
        status: ScheduledStatus,
        executed_at: datetime,
    ) -> None:
        """Persist a task status update in a single place."""

        task.status = status
        task.executed_at = executed_at
        session.add(task)
        session.commit()

    def _monitor_ongoing_schedules(self) -> None:
        """
        Ensure schedules exist for upcoming releases and metadata refreshes.

        Decomposed into helpers for clarity:
        - schedule upcoming episodes
        - schedule upcoming movies (known release date)
        - schedule ongoing/unreleased shows (computed next air)
        - schedule unknown-date movies (daily reindex)
        """

        offset_seconds = settings_manager.settings.indexer.schedule_offset_minutes * 60
        now = datetime.now()

        try:
            with db_session() as session:
                self._schedule_upcoming_episodes(session, now, offset_seconds)
                self._schedule_upcoming_movies(session, now, offset_seconds)
                self._schedule_ongoing_shows(session, now)
                self._schedule_unknown_movies(session, now)
        except Exception as e:
            logger.error(f"Monitor ongoing schedules failed: {e}")

    def _has_future_task(
        self,
        session: Session,
        item_id: int,
        task_type: str,
        now: datetime,
    ) -> bool:
        """Return True if a pending future task of this type already exists for item."""

        existing = (
            session.execute(
                select(ScheduledTask)
                .where(ScheduledTask.item_id == item_id)
                .where(ScheduledTask.task_type == task_type)
                .where(ScheduledTask.status == ScheduledStatus.Pending)
                .where(ScheduledTask.scheduled_for >= now)
                .limit(1)
            )
            .scalars()
            .first()
        )

        return existing is not None

    def _schedule_upcoming_episodes(
        self,
        session: Session,
        now: datetime,
        offset_seconds: int,
    ) -> None:
        """Schedule episode_release for future-dated episodes that are not completed."""

        upcoming_eps = (
            session.execute(
                select(Episode)
                .where(Episode.aired_at.is_not(None))
                .where(Episode.aired_at >= now)
                .where(~(Episode.last_state == States.Completed))
            )
            .unique()
            .scalars()
            .all()
        )

        for ep in upcoming_eps:
            if (
                not self._has_future_task(session, ep.id, "episode_release", now)
                and ep.aired_at
            ):
                run_at = ep.aired_at + timedelta(seconds=offset_seconds)

                try:
                    ep.schedule(
                        run_at,
                        task_type="episode_release",
                        offset_seconds=offset_seconds,
                        reason="monitor:episode_air",
                    )
                except Exception as e:
                    logger.debug(f"Skipping schedule for {ep.log_string}: {e}")

    def _schedule_upcoming_movies(
        self, session: Session, now: datetime, offset_seconds: int
    ) -> None:
        """Schedule movie_release for future-dated movies that are not completed."""

        upcoming_movies = (
            session.execute(
                select(Movie)
                .where(Movie.aired_at.is_not(None))
                .where(Movie.aired_at >= now)
                .where(~(Movie.last_state == States.Completed))
            )
            .unique()
            .scalars()
            .all()
        )
        for mv in upcoming_movies:
            if (
                not self._has_future_task(
                    session=session,
                    item_id=mv.id,
                    task_type="movie_release",
                    now=now,
                )
                and mv.aired_at
            ):
                run_at = mv.aired_at + timedelta(seconds=offset_seconds)

                try:
                    mv.schedule(
                        run_at=run_at,
                        task_type="movie_release",
                        offset_seconds=offset_seconds,
                        reason="monitor:movie_release",
                    )
                except Exception as e:
                    logger.debug(f"Skipping schedule for {mv.log_string}: {e}")

    def _schedule_ongoing_shows(self, session: Session, now: datetime) -> None:
        """Schedule reindex_show for ongoing/unreleased shows based on next air, with daily fallback."""

        ongoing_shows = (
            session.execute(
                select(Show).where(
                    or_(
                        Show.last_state.in_([States.Ongoing, States.Unreleased]),
                        # Patch 0025: also re-walk shows TVDB still lists as
                        # airing (Continuing/Upcoming) even if they fell to
                        # Completed. A show that finishes its current season
                        # goes Completed and drops out of this reindex
                        # schedule, so a newly-announced next season is never
                        # discovered (only an external content-source re-add
                        # brings it back). Keep re-indexing while TVDB says
                        # the series is still going.
                        func.lower(Show.tvdb_status).in_(
                            ["continuing", "upcoming"]
                        ),
                    )
                )
            )
            .unique()
            .scalars()
            .all()
        )

        for show in ongoing_shows:
            rd = show.release_data
            next_air = self._compute_next_air_datetime(rd, now)

            if next_air and next_air > now:
                if not self._has_future_task(session, show.id, "reindex_show", now):
                    try:
                        show.schedule(
                            next_air,
                            task_type="reindex_show",
                            reason="monitor:next_air",
                        )
                    except Exception as e:
                        logger.debug(
                            f"Skipping reindex schedule for {show.log_string}: {e}"
                        )
            else:
                fallback_time = (now + timedelta(days=1)).replace(
                    minute=0,
                    second=0,
                    microsecond=0,
                )

                if not self._has_future_task(session, show.id, "reindex_show", now):
                    try:
                        show.schedule(
                            fallback_time,
                            task_type="reindex_show",
                            reason="monitor:fallback_daily",
                        )
                    except Exception as e:
                        logger.debug(
                            f"Skipping fallback reindex for {show.log_string}: {e}"
                        )

    def _schedule_unknown_movies(self, session: Session, now: datetime) -> None:
        """Schedule daily reindex for movies without any known release date."""

        unknown_movies = (
            session.execute(
                select(Movie)
                .where(Movie.aired_at.is_(None))
                .where(
                    Movie.last_state.in_(
                        [
                            States.Unreleased,
                            States.Indexed,
                            States.Requested,
                            States.Unknown,
                        ]
                    )
                )
            )
            .unique()
            .scalars()
            .all()
        )

        for mv in unknown_movies:
            fallback_time = (now + timedelta(days=1)).replace(
                minute=0, second=0, microsecond=0
            )

            if not self._has_future_task(session, mv.id, "reindex_movie", now):
                try:
                    mv.schedule(
                        fallback_time,
                        task_type="reindex_movie",
                        reason="monitor:fallback_daily",
                    )
                except Exception as e:
                    logger.debug(f"Skipping fallback reindex for {mv.log_string}: {e}")

    @staticmethod
    def _compute_next_air_datetime(
        release_data: SeriesRelease | None,
        ref: datetime,
    ) -> datetime | None:
        """Compute the next air datetime from a TVDB-like payload.

        Strategy:
        1) Try explicit next_aired (date or datetime). If date-only, combine with airs_time.
        2) Otherwise, use airs_days + airs_time to find the next matching weekday.
        All times honor release_data['timezone'] when provided, then converted to local naive.
        """

        if not release_data:
            return None

        dt = ProgramScheduler._parse_next_aired_datetime(release_data)

        if dt is not None and dt >= ref:
            return dt

        # Fall through to weekday computation if next_aired is in the past
        hm = ProgramScheduler._parse_airs_time(release_data.airs_time)

        if hm is None:
            return None

        hour, minute = hm

        valid_days = ProgramScheduler._valid_weekdays(release_data.airs_days)

        if not valid_days:
            return None

        # Find next occurrence >= ref within 3 weeks
        for i in range(0, 21):
            candidate = ref + timedelta(days=i)

            if candidate.weekday() in valid_days:
                candidate_dt = candidate.replace(
                    hour=hour,
                    minute=minute,
                    second=0,
                    microsecond=0,
                )

                if candidate_dt and candidate_dt >= ref:
                    return candidate_dt

        return None

    @staticmethod
    def _parse_next_aired_datetime(release_data: SeriesRelease) -> datetime | None:
        """Parse release_data['next_aired'] into a datetime, combining with airs_time if needed."""

        next_aired = release_data.next_aired

        if not next_aired:
            return None

        # If datetime-like
        if "T" in next_aired or " " in next_aired:
            try:
                return datetime.fromisoformat(next_aired)
            except Exception:
                return None

        airs_time = release_data.airs_time

        # Date-only
        try:
            base = datetime.fromisoformat(next_aired + "T00:00:00")

            if airs_time:
                try:
                    hour, minute = [int(x) for x in str(airs_time).split(":", 1)]
                except Exception:
                    hour, minute = 0, 0

                return base.replace(hour=hour, minute=minute)

            return base
        except Exception:
            return None

    @staticmethod
    def _parse_airs_time(airs_time: str | None) -> tuple[int, int] | None:
        """Parse HH:MM from release_data['airs_time'] if present and valid."""

        if not airs_time:
            return None

        try:
            hour, minute = [int(x) for x in str(airs_time).split(":", 1)]
            return hour, minute
        except Exception:
            return None

    @staticmethod
    def _valid_weekdays(series_airs_days: SeriesAirsDays | None) -> list[int]:
        """Return list of weekday indices [0..6] marked True in release_data['airs_days']."""

        if not series_airs_days:
            return []

        day_map = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ]

        return [
            i
            for i, name in enumerate(day_map)
            if getattr(series_airs_days, name) is True
        ]
