import os

from collections.abc import Callable, Sequence
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Literal, Self
from fastapi import APIRouter, Body, HTTPException, Path, status, Query
from kink import di
from loguru import logger
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session, object_session

from program.db import db_functions
from program.db.db import db_session
from program.media.item import Episode, MediaItem, Movie, Season, Show
from program.media.state import States
from program.types import Event
from program.program import Program
from program.media.models import MediaMetadata
from program.settings import settings_manager

from ..models.shared import IdListPayload, MessageResponse


class MediaTypeEnum(str, Enum):
    MOVIE = "movie"
    SHOW = "show"
    SEASON = "season"
    EPISODE = "episode"
    ANIME = "anime"


class SortOrderEnum(str, Enum):
    TITLE_ASC = "title_asc"
    TITLE_DESC = "title_desc"
    DATE_ASC = "date_asc"
    DATE_DESC = "date_desc"

    @property
    def sort_type(self) -> str:
        return "title" if self.value.startswith("title") else "date"


router = APIRouter(
    prefix="/items",
    tags=["items"],
    responses={404: {"description": "Not found"}},
)


def handle_ids(ids: Sequence[str | int]) -> list[int]:
    try:
        id_list = [int(id) for id in ids]

        if not id_list:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No item ID provided",
            )

        return id_list
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid item ID(s) provided",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing item ID(s): {str(e)}",
        ) from e


# Convenience helper to mutate an item and update states consistently
def apply_item_mutation(
    program: Program,
    session: Session,
    item: MediaItem,
    mutation_fn: "Callable[[MediaItem, Session], None]",
    bubble_parents: bool = True,
) -> None:
    """Cancel jobs, apply mutation, then update item and ancestor states.
    - Uses base MediaItem.store_state to avoid recursive child updates for seasons/shows.
    - Caller is responsible for session.commit().
    """

    try:
        program.em.cancel_job(item.id)
    except Exception:
        logger.debug(f"No active job to cancel for item {getattr(item, 'id', None)}")

    # Ensure attached instance
    if object_session(item) is not session:
        item = session.merge(item)

    # Apply mutation
    mutation_fn(item, session)

    # Update self state (non-recursive)
    try:
        MediaItem.store_state(item)
    except Exception as e:
        logger.warning(f"Failed to store state for {item.id}: {e}")

    if not bubble_parents:
        return

    # Update parent states (non-recursive)
    try:
        if isinstance(item, Episode):
            season = session.get(Season, item.parent_id)

            if season:
                MediaItem.store_state(season)
                show = session.get(Show, season.parent_id)

                if show:
                    MediaItem.store_state(show)
        elif isinstance(item, Season):
            show = session.get(Show, item.parent_id)

            if show:
                MediaItem.store_state(show)
    except Exception as e:
        logger.warning(f"Failed to update parent state(s) for item {item.id}: {e}")


class StateResponse(BaseModel):
    success: Annotated[
        bool,
        Field(description="Boolean signifying whether the request was successful"),
    ]
    states: Annotated[
        list[str],
        Field(description="The list of states"),
    ]


@router.get(
    "/states",
    operation_id="get_states",
    response_model=StateResponse,
)
async def get_states() -> StateResponse:
    return StateResponse(states=[state._name_ for state in States], success=True)


# ---------------------------------------------------------------------------
# Patch 0038: per-item blacklisted releases (StreamBlacklistRelation)
#
# There are TWO unrelated blacklists in this deployment:
#   * the GLOBAL infohash blocklist -- filesystem.excluded_items.infohashes,
#     written only by patch 0029's "Blocklist this file"; already on /blocklist.
#   * the PER-ITEM stream blacklist -- the StreamBlacklistRelation table,
#     written automatically by patch 0002 on every RD 451 and by
#     MediaItem.blacklist_active_stream(). Nothing rendered it, so it silently
#     grew to thousands of rows.
#
# This endpoint makes the second one readable. Removal reuses the stock
# POST /items/{item_id}/streams/{stream_id}/unblacklist -- no new write path.
#
# NOTE ON ROUTE ORDER: this must stay ABOVE `GET /{id}` (Starlette matches in
# registration order and `/{id}` would swallow `/blacklisted_streams`).
# ---------------------------------------------------------------------------


class BlacklistedStreamEntry(BaseModel):
    relation_id: Annotated[
        int,
        Field(description="StreamBlacklistRelation row id (ordering key)"),
    ]
    stream_id: Annotated[
        int,
        Field(description="Stream id -- pass to /streams/{stream_id}/unblacklist"),
    ]
    infohash: Annotated[str, Field(description="Torrent infohash")]
    raw_title: Annotated[str, Field(description="Release name as scraped")]
    parsed_title: Annotated[str | None, Field(description="RTN parsed title")] = None
    rank: Annotated[int | None, Field(description="RTN rank")] = None
    resolution: Annotated[str | None, Field(description="Parsed resolution")] = None
    flagged_451_services: Annotated[
        list[str],
        Field(
            description=(
                "Debrid services that returned 451 for this hash (patch 0011). "
                "Non-empty means the 451 gate already skips it, so the blacklist "
                "row is redundant."
            )
        ),
    ] = []


class BlacklistedItemGroup(BaseModel):
    item_id: Annotated[int, Field(description="Riven MediaItem id")]
    title: Annotated[str, Field(description="Display title (log_string shape)")]
    type: Annotated[str, Field(description="movie / show / season / episode")]
    state: Annotated[str | None, Field(description="last_state of the item")] = None
    poster_path: Annotated[str | None, Field(description="Poster, item or root")] = None
    root_id: Annotated[
        int | None,
        Field(description="Top-level ancestor MediaItem id (show or movie)"),
    ] = None
    root_type: Annotated[str | None, Field(description="show / movie")] = None
    root_title: Annotated[str | None, Field(description="Ancestor title")] = None
    tvdb_id: Annotated[str | None, Field(description="Ancestor tvdb id (shows)")] = None
    tmdb_id: Annotated[str | None, Field(description="Ancestor tmdb id (movies)")] = None
    imdb_id: Annotated[str | None, Field(description="Ancestor imdb id")] = None
    count: Annotated[int, Field(description="Blacklisted releases in this group")]
    newest_relation_id: Annotated[
        int,
        Field(description="Highest relation id in the group (sort key)"),
    ]
    streams: Annotated[
        list[BlacklistedStreamEntry],
        Field(description="The blacklisted releases, newest first"),
    ]


class BlacklistedStreamsResponse(BaseModel):
    success: Annotated[bool, Field(description="Request succeeded")]
    page: Annotated[int, Field(description="Current page number")]
    limit: Annotated[int, Field(description="Items (not releases) per page")]
    total_items: Annotated[int, Field(description="Distinct items after filters")]
    total_pages: Annotated[int, Field(description="Total pages after filters")]
    total_relations: Annotated[
        int,
        Field(description="Blacklist rows after filters"),
    ]
    hidden_451: Annotated[
        int,
        Field(
            description=(
                "Rows matching the search that are hidden because their stream is "
                "451-flagged. Reported even when include_451 is true, so the UI can "
                "label the toggle."
            )
        ),
    ]
    items: Annotated[list[BlacklistedItemGroup], Field(description="One group per item")]


def _bl_state_name(value: Any) -> str | None:
    if value is None:
        return None
    return getattr(value, "name", None) or str(value)


def _bl_flags(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value]
    return []


@router.get(
    "/blacklisted_streams",
    summary="List Per-Item Blacklisted Streams",
    description=(
        "Paginated view of the per-item stream blacklist (StreamBlacklistRelation), "
        "grouped one entry per media item and ordered newest-first by row id -- the "
        "table has no timestamp. This is NOT the global infohash blocklist "
        "(filesystem.excluded_items.infohashes); the two are unrelated. "
        "Remove an entry with POST /items/{item_id}/streams/{stream_id}/unblacklist."
    ),
    operation_id="get_blacklisted_streams",
    response_model=BlacklistedStreamsResponse,
)
async def get_blacklisted_streams(
    page: Annotated[
        int,
        Query(description="Page number", ge=1),
    ] = 1,
    limit: Annotated[
        int,
        Query(description="Items per page", ge=1, le=100),
    ] = 25,
    search: Annotated[
        str | None,
        Query(description="Case-insensitive match on the item or its show title"),
    ] = None,
    include_451: Annotated[
        bool,
        Query(
            description=(
                "Include releases whose stream is already 451-flagged. These are "
                "skipped by the 451 gate anyway, so they default to hidden."
            )
        ),
    ] = False,
) -> BlacklistedStreamsResponse:
    from sqlalchemy import or_

    from program.media.stream import Stream, StreamBlacklistRelation

    rel_t = StreamBlacklistRelation.__table__
    stream_t = Stream.__table__
    item_t = MediaItem.__table__
    ep_t = Episode.__table__
    season_of_ep = Season.__table__.alias("bl_season_of_ep")
    season_self = Season.__table__.alias("bl_season_self")
    root_t = MediaItem.__table__.alias("bl_root")

    # MediaItem -> (season of episode | season itself) -> top-level show, or the
    # item itself for movies/shows. Joined-inheritance means parent_id lives on
    # the Episode/Season child tables, not on MediaItem.
    root_id_expr = func.coalesce(
        season_of_ep.c.parent_id,
        season_self.c.parent_id,
        item_t.c.id,
    )

    joined = (
        rel_t.join(stream_t, stream_t.c.id == rel_t.c.stream_id)
        .join(item_t, item_t.c.id == rel_t.c.media_item_id)
        .outerjoin(ep_t, ep_t.c.id == item_t.c.id)
        .outerjoin(season_of_ep, season_of_ep.c.id == ep_t.c.parent_id)
        .outerjoin(season_self, season_self.c.id == item_t.c.id)
        .outerjoin(root_t, root_t.c.id == root_id_expr)
    )

    not_flagged = func.json_array_length(stream_t.c.flagged_451_services) == 0

    search_clauses = []
    term = (search or "").strip()

    if term:
        like = f"%{term}%"
        search_clauses.append(
            or_(item_t.c.title.ilike(like), root_t.c.title.ilike(like))
        )

    filters = list(search_clauses)

    if not include_451:
        filters.append(not_flagged)

    with db_session() as session:
        total_relations = (
            session.execute(
                select(func.count()).select_from(joined).where(*filters)
            ).scalar_one()
            or 0
        )
        total_items = (
            session.execute(
                select(func.count(rel_t.c.media_item_id.distinct()))
                .select_from(joined)
                .where(*filters)
            ).scalar_one()
            or 0
        )
        hidden_451 = (
            session.execute(
                select(func.count())
                .select_from(joined)
                .where(*search_clauses, ~not_flagged)
            ).scalar_one()
            or 0
        )

        groups = session.execute(
            select(
                rel_t.c.media_item_id.label("mid"),
                func.max(rel_t.c.id).label("newest"),
                func.count().label("cnt"),
            )
            .select_from(joined)
            .where(*filters)
            .group_by(rel_t.c.media_item_id)
            .order_by(func.max(rel_t.c.id).desc())
            .limit(limit)
            .offset((page - 1) * limit)
        ).all()

        item_ids = [row.mid for row in groups]

        if not item_ids:
            return BlacklistedStreamsResponse(
                success=True,
                page=page,
                limit=limit,
                total_items=total_items,
                total_pages=(total_items + limit - 1) // limit,
                total_relations=total_relations,
                hidden_451=hidden_451,
                items=[],
            )

        meta_filters = [item_t.c.id.in_(item_ids)]
        meta_rows = session.execute(
            select(
                item_t.c.id,
                item_t.c.title,
                item_t.c.type,
                item_t.c.last_state,
                item_t.c.poster_path,
                ep_t.c.number.label("episode_number"),
                season_of_ep.c.number.label("episode_season_number"),
                season_self.c.number.label("season_number"),
                root_t.c.id.label("root_id"),
                root_t.c.type.label("root_type"),
                root_t.c.title.label("root_title"),
                root_t.c.poster_path.label("root_poster_path"),
                root_t.c.tvdb_id.label("root_tvdb_id"),
                root_t.c.tmdb_id.label("root_tmdb_id"),
                root_t.c.imdb_id.label("root_imdb_id"),
            )
            .select_from(
                item_t.outerjoin(ep_t, ep_t.c.id == item_t.c.id)
                .outerjoin(season_of_ep, season_of_ep.c.id == ep_t.c.parent_id)
                .outerjoin(season_self, season_self.c.id == item_t.c.id)
                .outerjoin(root_t, root_t.c.id == root_id_expr)
            )
            .where(*meta_filters)
        ).all()
        meta_by_id = {row.id: row for row in meta_rows}

        stream_filters = [rel_t.c.media_item_id.in_(item_ids)]

        if not include_451:
            stream_filters.append(not_flagged)

        stream_rows = session.execute(
            select(
                rel_t.c.id.label("relation_id"),
                rel_t.c.media_item_id.label("mid"),
                stream_t.c.id.label("stream_id"),
                stream_t.c.infohash,
                stream_t.c.raw_title,
                stream_t.c.parsed_title,
                stream_t.c.rank,
                stream_t.c.resolution,
                stream_t.c.flagged_451_services,
            )
            .select_from(rel_t.join(stream_t, stream_t.c.id == rel_t.c.stream_id))
            .where(*stream_filters)
            .order_by(rel_t.c.id.desc())
        ).all()

    streams_by_item: dict[int, list[BlacklistedStreamEntry]] = {}

    for row in stream_rows:
        streams_by_item.setdefault(row.mid, []).append(
            BlacklistedStreamEntry(
                relation_id=row.relation_id,
                stream_id=row.stream_id,
                infohash=row.infohash,
                raw_title=row.raw_title,
                parsed_title=row.parsed_title,
                rank=row.rank,
                resolution=row.resolution,
                flagged_451_services=_bl_flags(row.flagged_451_services),
            )
        )

    items: list[BlacklistedItemGroup] = []

    for group in groups:
        meta = meta_by_id.get(group.mid)

        if meta is None:
            continue

        root_title = meta.root_title or meta.title or f"Item {group.mid}"

        if meta.type == "episode" and meta.episode_number is not None:
            season_number = meta.episode_season_number or 0
            title = f"{root_title} S{season_number:02}E{meta.episode_number:02}"
        elif meta.type == "season" and meta.season_number is not None:
            title = f"{root_title} S{meta.season_number:02}"
        else:
            title = meta.title or root_title

        items.append(
            BlacklistedItemGroup(
                item_id=group.mid,
                title=title,
                type=meta.type,
                state=_bl_state_name(meta.last_state),
                poster_path=meta.poster_path or meta.root_poster_path,
                root_id=meta.root_id,
                root_type=meta.root_type,
                root_title=meta.root_title,
                tvdb_id=meta.root_tvdb_id,
                tmdb_id=meta.root_tmdb_id,
                imdb_id=meta.root_imdb_id,
                count=group.cnt,
                newest_relation_id=group.newest,
                streams=streams_by_item.get(group.mid, []),
            )
        )

    return BlacklistedStreamsResponse(
        success=True,
        page=page,
        limit=limit,
        total_items=total_items,
        total_pages=(total_items + limit - 1) // limit,
        total_relations=total_relations,
        hidden_451=hidden_451,
        items=items,
    )


class ItemsResponse(BaseModel):
    success: Annotated[
        bool,
        Field(description="Boolean signifying whether the request was successful"),
    ]
    items: Annotated[
        list[dict[str, Any]],
        Field(description="The list of media items"),
    ]
    page: Annotated[
        int,
        Field(description="Current page number"),
    ]
    limit: Annotated[
        int,
        Field(description="Number of items per page"),
    ]
    total_items: Annotated[
        int,
        Field(description="Total number of items"),
    ]
    total_pages: Annotated[
        int,
        Field(description="Total number of pages"),
    ]


class StatesFilter(str, Enum):
    All = "All"


@router.get(
    "",
    summary="Search Media Items",
    description="Fetch media items with optional filters and pagination",
    operation_id="get_items",
    response_model=ItemsResponse,
)
async def get_items(
    limit: Annotated[
        int,
        Query(
            description="Number of items per page",
            ge=1,
        ),
    ] = 50,
    page: Annotated[
        int,
        Query(
            description="Page number",
            ge=1,
        ),
    ] = 1,
    type: Annotated[
        list[MediaTypeEnum] | None,
        Query(description="Filter by media type(s)"),
    ] = None,
    states: Annotated[
        list[States | StatesFilter] | None,
        Query(description="Filter by state(s)"),
    ] = None,
    sort: Annotated[
        list[SortOrderEnum] | None,
        Query(
            description="Sort order(s). Multiple sorts allowed but only one per type (title or date)"
        ),
    ] = None,
    search: Annotated[
        str | None,
        Query(
            description="Search by title or IMDB/TVDB/TMDB ID",
            min_length=1,
        ),
    ] = None,
    extended: Annotated[
        bool,
        Query(description="Include extended item details"),
    ] = False,
) -> ItemsResponse:
    query = select(MediaItem)

    if search:
        search_lower = search.lower()

        if search_lower.startswith("tt"):
            query = query.where(MediaItem.imdb_id == search_lower)
        elif search_lower.startswith("tmdb_"):
            tmdb_id = search_lower.replace("tmdb_", "")
            query = query.where(MediaItem.tmdb_id == tmdb_id)
        elif search_lower.startswith("tvdb_"):
            tvdb_id = search_lower.replace("tvdb_", "")
            query = query.where(MediaItem.tvdb_id == tvdb_id)
        else:
            query = query.where(func.lower(MediaItem.title).like(f"%{search_lower}%"))

    if states and StatesFilter.All not in states:
        query = query.where(
            MediaItem.last_state.in_([s for s in states if isinstance(s, States)])
        )

    if type:
        media_types = {t.value for t in type}

        if MediaTypeEnum.ANIME in type:
            media_types.remove(MediaTypeEnum.ANIME.value)

            if not media_types:
                query = query.where(MediaItem.is_anime == True)
            else:
                query = query.where(
                    and_(
                        MediaItem.type.in_(
                            media_types if media_types else ["movie", "show"]
                        ),
                        MediaItem.is_anime == True,
                    )
                )

        elif media_types:
            query = query.where(MediaItem.type.in_(media_types))

    if sort:
        # Verify we don't have multiple sorts of the same type
        sort_types = set[str]()

        for sort_criterion in sort:
            sort_type = sort_criterion.sort_type

            if sort_type in sort_types:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Multiple {sort_type} sort criteria provided. Only one sort per type is allowed.",
                )

            sort_types.add(sort_type)

        for sort_criterion in sort:
            if sort_criterion == SortOrderEnum.TITLE_ASC:
                query = query.order_by(MediaItem.title.asc())
            elif sort_criterion == SortOrderEnum.TITLE_DESC:
                query = query.order_by(MediaItem.title.desc())
            elif sort_criterion == SortOrderEnum.DATE_ASC:
                query = query.order_by(MediaItem.requested_at.asc())
            elif sort_criterion == SortOrderEnum.DATE_DESC:
                query = query.order_by(MediaItem.requested_at.desc())

    else:
        query = query.order_by(MediaItem.requested_at.desc())

    with db_session() as session:
        total_items = session.execute(
            select(func.count()).select_from(query.subquery())
        ).scalar_one()

        items = (
            session.execute(query.offset((page - 1) * limit).limit(limit))
            .unique()
            .scalars()
            .all()
        )

        total_pages = (total_items + limit - 1) // limit

        return ItemsResponse(
            success=True,
            items=[
                item.to_extended_dict() if extended else item.to_dict()
                for item in items
            ],
            page=page,
            limit=limit,
            total_items=total_items,
            total_pages=total_pages,
        )


class AddMediaItemPayload(BaseModel):
    tmdb_ids: Annotated[
        list[str] | None,
        Field(
            default=None,
            description="Comma-separated list of TMDB IDs",
        ),
    ]
    tvdb_ids: Annotated[
        list[str] | None,
        Field(
            default=None,
            description="Comma-separated list of TVDB IDs",
        ),
    ]
    media_type: Annotated[
        Literal["movie", "tv"],
        Field(description="Media type"),
    ]


@router.post(
    "/add",
    summary="Add Media Items",
    description="""
        Add media items with bases on TMDB ID or TVDB ID,
        you can add multiple IDs by comma separating them.
    """,
    operation_id="add_items",
    response_model=MessageResponse,
)
async def add_items(
    payload: Annotated[
        AddMediaItemPayload,
        Body(description="Add media items payload"),
    ],
) -> MessageResponse:
    if not payload.tmdb_ids and not payload.tvdb_ids:
        raise HTTPException(status_code=400, detail="No ID(s) provided")

    all_tmdb_ids = (
        [id.strip() for id in payload.tmdb_ids if id]
        if payload.tmdb_ids and payload.media_type == "movie"
        else None
    )

    all_tvdb_ids = (
        [id.strip() for id in payload.tvdb_ids if id]
        if payload.tvdb_ids and payload.media_type == "tv"
        else None
    )

    added_count = 0
    items = list[MediaItem]()

    with db_session() as session:
        if all_tmdb_ids:
            for id in all_tmdb_ids:
                # Check if item exists using ORM
                existing = session.execute(
                    select(MediaItem).where(MediaItem.tmdb_id == id)
                ).scalar_one_or_none()

                if not existing:
                    item = MediaItem(
                        {
                            "tmdb_id": id,
                            "requested_by": "riven",
                            "requested_at": datetime.now(),
                        }
                    )

                    if item:
                        items.append(item)
                else:
                    logger.debug(f"Item with TMDB ID {id} already exists")

        if all_tvdb_ids:
            for id in all_tvdb_ids:
                # Check if item exists using ORM
                existing = session.execute(
                    select(MediaItem).where(MediaItem.tvdb_id == id)
                ).scalar_one_or_none()

                if not existing:
                    item = MediaItem(
                        {
                            "tvdb_id": id,
                            "requested_by": "riven",
                            "requested_at": datetime.now(),
                        }
                    )
                    if item:
                        items.append(item)
                else:
                    logger.debug(f"Item with TVDB ID {id} already exists")

        if items:
            for item in items:
                di[Program].em.add_item(item)
                added_count += 1

    return MessageResponse(message=f"Added {added_count} item(s) to the queue")


@router.get(
    "/{id}",
    summary="Get Media Item by ID",
    description="Fetch a single media item by item ID",
    operation_id="get_item",
)
async def get_item(
    id: Annotated[
        str,
        Path(
            description="""
                The ID of the media item. For 'item' type, use the numeric item ID;
                for 'movie' or 'tv' types, use the TMDB or TVDB ID respectively.
            """,
        ),
    ],
    media_type: Annotated[
        Literal["movie", "tv", "item"],
        Query(description="The type of media item"),
    ],
    extended: Annotated[
        bool,
        Query(description="Whether to include extended information"),
    ] = False,
) -> dict[str, Any]:
    if not id:
        raise HTTPException(status_code=400, detail="No ID or media type provided")

    with db_session() as session:
        match media_type:
            case "movie":
                # needs to be a string
                # Constrain to the requested TYPE. TMDB gives movies and shows
                # separate id spaces, so a bare tmdb_id match can also hit a show.
                query = select(MediaItem).where(
                    MediaItem.tmdb_id == id,
                    MediaItem.type == "movie",
                )
            case "tv":
                # needs to be a string
                #
                # TVDB numbers SERIES and EPISODES in separate id spaces, so the
                # same integer is routinely both a valid series id and a valid
                # episode id. Without a type filter this matched seasons and
                # episodes too, and `scalar_one_or_none()` then raised, which the
                # handler below turns into a 500:
                #
                #   {"detail":"Multiple items found with ID 446718: {87482, 54227}"}
                #
                # 87482 is the SHOW "Tires"; 54227 is an unrelated EPISODE that
                # merely shares the number. The frontend detail loader catches
                # that failure and falls back to its not-in-library branch, so an
                # item that IS in the library renders as "Request" with every
                # action missing. Measured on this deployment: 8 shows affected
                # out of 1,018, against 42,911 episodes carrying a tvdb_id.
                query = select(MediaItem).where(
                    MediaItem.tvdb_id == id,
                    MediaItem.type == "show",
                )
            case "item":
                # needs to be an integer
                _id = int(id)
                query = select(MediaItem).where(
                    MediaItem.id == _id,
                )

        try:
            item = session.execute(query).unique().scalar_one_or_none()

            if not item:
                raise HTTPException(status_code=404, detail="Item not found")

            if extended:
                return item.to_extended_dict()

            return item.to_dict()
        except Exception as e:
            # Handle multiple results
            if "Multiple rows were found when one or none was required" in str(e):
                items = session.execute(query).unique().scalars().all()
                duplicate_ids = {item.id for item in items}
                logger.debug(f"Multiple items found with ID {id}: {duplicate_ids}")

                raise HTTPException(
                    status_code=500,
                    detail=f"Multiple items found with ID {id}: {duplicate_ids}",
                )

            logger.error(f"Error fetching item with ID {id}: {str(e)}")

            raise HTTPException(status_code=500, detail=str(e)) from e


_SKIP_RESET_STATES = frozenset({
    States.Completed, States.Unreleased,
    States.Downloaded, States.Symlinked,
    States.Paused,
})


def _reset_scrape_state(item: MediaItem) -> None:
    """Recursively reset scraping state on incomplete children only.

    Walks Show → Season → Episode and clears scraping metadata, streams,
    and failed_attempts on any item not in _SKIP_RESET_STATES, then sets
    its state to Indexed so it re-enters the scraping pipeline.

    Completed / Downloaded / Symlinked / Unreleased / Paused items are
    left untouched.
    """
    if item.last_state in _SKIP_RESET_STATES:
        return

    item.scraped_at = None
    item.scraped_times = 0
    item.failed_attempts = 0
    item.streams.clear()
    item.blacklisted_streams.clear()
    item.active_stream = None
    MediaItem.store_state(item, States.Indexed)

    if isinstance(item, Show):
        for season in item.seasons:
            _reset_scrape_state(season)
    elif isinstance(item, Season):
        for episode in item.episodes:
            _reset_scrape_state(episode)


class ResetResponse(MessageResponse):
    ids: list[int]


@router.post(
    "/reset",
    summary="Reset Media Items",
    description="Reset media items with bases on item IDs",
    operation_id="reset_items",
    response_model=ResetResponse,
)
async def reset_items(
    payload: Annotated[
        IdListPayload,
        Body(description="Reset items payload"),
    ],
) -> ResetResponse:
    """
    Reset the specified media items to their initial state and trigger a media-server library refresh when applicable.

    Parameters:
        request (Request): FastAPI request object used to access application services.
        ids (str): Comma-separated list of item IDs (e.g., "1,2,3") to reset.

    Returns:
        ResetResponse: Dictionary with a human-readable message and the list of processed item IDs:
            - message (str): Summary of the performed reset.
            - ids (list[int]): The numeric IDs that were processed.

    Raises:
        HTTPException: Raised with status 400 when the provided `ids` string cannot be parsed into valid IDs.
    """

    parsed_ids = handle_ids(payload.ids)

    services = di[Program].services

    assert services, "Program services not initialized"

    # Get updater service for media server refresh
    updater = services.updater

    try:
        # Load items using ORM
        with db_session() as session:
            items = (
                session.execute(select(MediaItem).where(MediaItem.id.in_(parsed_ids)))
                .scalars()
                .all()
            )

            for media_item in items:
                try:
                    # Gather all refresh paths before reset (entry may appear at multiple VFS paths)
                    refresh_paths = list[str]()

                    media_entry = media_item.media_entry

                    if updater and media_entry:
                        vfs_paths = media_entry.get_all_vfs_paths()

                        for vfs_path in vfs_paths:
                            abs_path = os.path.join(
                                updater.library_path, vfs_path.lstrip("/")
                            )

                            if isinstance(media_item, Movie):
                                refresh_path = os.path.dirname(
                                    os.path.dirname(abs_path)
                                )
                            else:  # show
                                refresh_path = os.path.dirname(
                                    os.path.dirname(os.path.dirname(abs_path))
                                )
                            if refresh_path not in refresh_paths:
                                refresh_paths.append(refresh_path)

                    def mutation(i: MediaItem, s: Session):
                        """
                        Blacklist the MediaItem's currently active stream and reset the item's state.

                        Parameters:
                            i (MediaItem): The item to mutate.
                            s (Session): Database session (provided for caller context; not used directly here).
                        """

                        i.blacklist_active_stream()
                        if isinstance(i, (Show, Season)):
                            _reset_scrape_state(i)
                        else:
                            i.reset()

                    apply_item_mutation(
                        di[Program],
                        session,
                        media_item,
                        mutation,
                        bubble_parents=True,
                    )

                    session.commit()

                    # Trigger media server refresh for all paths where this item appeared
                    if updater and updater.initialized:
                        for refresh_path in refresh_paths:
                            updater.refresh_path(refresh_path)
                            logger.debug(
                                f"Triggered media server refresh for {refresh_path}"
                            )

                except ValueError as e:
                    logger.error(
                        f"Failed to reset item with id {media_item.id}: {str(e)}"
                    )
                    continue
                except Exception as e:
                    logger.error(
                        f"Unexpected error while resetting item with id {media_item.id}: {str(e)}"
                    )
                    continue
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return ResetResponse(
        message=f"Reset items with id {parsed_ids}",
        ids=parsed_ids,
    )


class RetryResponse(MessageResponse):
    ids: Annotated[
        Sequence[int],
        Field(description="The IDs to retry", min_length=1),
    ]


@router.post(
    "/retry",
    summary="Retry Media Items",
    description="Retry media items with bases on item IDs",
    operation_id="retry_items",
    response_model=RetryResponse,
)
async def retry_items(
    payload: Annotated[
        IdListPayload,
        Body(description="Retry items payload"),
    ],
) -> RetryResponse:
    """Re-add items to the queue"""

    parsed_ids = handle_ids(payload.ids)

    with db_session() as session:
        for id in parsed_ids:
            try:
                item = session.get(MediaItem, id)

                if item:

                    def mutation(i: MediaItem, s: Session):
                        _reset_scrape_state(i)

                    apply_item_mutation(
                        program=di[Program],
                        session=session,
                        item=item,
                        mutation_fn=mutation,
                        bubble_parents=True,
                    )

                    session.commit()

                    di[Program].em.add_event(Event("RetryItem", id))
            except ValueError as e:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
                )

    return RetryResponse(
        message=f"Retried items with ids {parsed_ids}",
        ids=parsed_ids,
    )


@router.post(
    "/retry_library",
    summary="Retry Library Items",
    description="Retry items in the library that failed to download",
    operation_id="retry_library_items",
    response_model=RetryResponse,
)
async def retry_library_items() -> RetryResponse:
    item_ids = db_functions.retry_library()

    for item_id in item_ids:
        di[Program].em.add_event(
            Event(
                emitted_by="RetryLibrary",
                item_id=item_id,
            )
        )

    return RetryResponse(
        message=f"Retried {len(item_ids)} items",
        ids=item_ids,
    )


class RemoveResponse(BaseModel):
    message: str
    ids: Annotated[
        list[int],
        Field(description="The IDs to remove"),
    ]


@router.delete(
    "/remove",
    summary="Remove Media Items",
    description="Remove media items based on item IDs",
    operation_id="remove_item",
    response_model=RemoveResponse,
)
async def remove_item(
    payload: Annotated[
        IdListPayload,
        Body(description="Remove items payload"),
    ],
) -> RemoveResponse:
    """
    Remove one or more media items identified by their IDs.

    Deletes the MediaItem rows and their related data (joined-table rows, hierarchical children, subtitles, and stream relations) and coordinates related side effects: cancels active jobs for the item, deletes an associated Overseerr request when present, and triggers a media server library refresh for the item's library path when an Updater service is available and initialized.

    Parameters:
        request (Request): FastAPI request object (used to access application services).
        ids (str): Comma-separated string of one or more numeric item IDs.

    Returns:
        dict: Response containing a human-readable message and the list of removed item IDs, e.g. {"message": "...", "ids": [1,2]}.

    Raises:
        HTTPException: If no IDs are provided or if an item type is not removable (only "movie" and "show" are allowed).
    """

    parsed_ids = handle_ids(payload.ids)

    if not parsed_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="No IDs provided"
        )

    services = di[Program].services

    assert services, "Program services not initialized"

    # Get services
    overseerr = services.overseerr
    updater = services.updater
    removed_ids = list[int]()

    with db_session() as session:
        for item_id in parsed_ids:
            # Load item using ORM
            item = session.get(MediaItem, item_id)

            if not item:
                logger.warning(f"Item {item_id} not found, skipping")
                continue

            # Patch 0019: allow Movie/Show/Season/Episode. Season/Episode
            # removal skips Overseerr (request lives on the parent show) and
            # uses a representative episode's filesystem entry for refresh
            # path computation.
            if not isinstance(item, (Movie, Show, Season, Episode)):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Item {item_id} is a {item.type} and cannot be removed",
                )

            logger.debug(f"Removing item with ID {item.id}")

            # 1. Cancel active jobs (EventManager cancels children too)
            di[Program].em.cancel_job(item.id)

            # 2. Gather all refresh paths before deletion (entry may appear at multiple VFS paths)
            refresh_paths = list[str]()

            # For Season, refresh from any child episode that has a file.
            # For Movie/Show/Episode, use the item's own filesystem_entry.
            refresh_source = item
            if isinstance(item, Season):
                refresh_source = next(
                    (e for e in item.episodes if e.filesystem_entry),
                    item,
                )

            if updater and refresh_source.filesystem_entry:
                if media_entry := refresh_source.media_entry:
                    for vfs_path in media_entry.get_all_vfs_paths():
                        # Check if VFS path is already absolute (filesystem path)
                        # VFS paths are normally VFS-relative (e.g., /movies/...) but could be
                        # absolute filesystem paths in some configurations
                        if os.path.isabs(vfs_path) and not vfs_path.startswith(
                            str(updater.library_path)
                        ):
                            # VFS path is absolute but not under library_path - use as-is
                            abs_path = vfs_path
                        elif os.path.isabs(vfs_path) and vfs_path.startswith(
                            str(updater.library_path)
                        ):
                            # VFS path is already an absolute path under library_path - use as-is
                            abs_path = vfs_path
                        else:
                            # VFS path is VFS-relative - join with library_path
                            abs_path = os.path.join(
                                updater.library_path, vfs_path.lstrip("/")
                            )

                        if isinstance(item, Movie):
                            refresh_path = os.path.dirname(os.path.dirname(abs_path))
                        else:  # Show / Season / Episode all live under show dir
                            refresh_path = os.path.dirname(
                                os.path.dirname(os.path.dirname(abs_path))
                            )
                        if refresh_path not in refresh_paths:
                            refresh_paths.append(refresh_path)

            # 3. Delete from Overseerr (only Movie/Show carry the request)
            if isinstance(item, (Movie, Show)) and item.overseerr_id and overseerr:
                try:
                    overseerr.api.delete_request(item.overseerr_id)

                    logger.debug(
                        f"Deleted Overseerr request {item.overseerr_id} for {item.id}"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to delete Overseerr request {item.overseerr_id}: {e}"
                    )

            # 4. Remove from VFS
            if services.filesystem.riven_vfs:
                services.filesystem.riven_vfs.remove(item)

            # 5. Delete from database using ORM
            session.delete(item)
            session.commit()

            removed_ids.append(item_id)

            logger.debug(f"Deleted item {item_id} from database")

            # 6. Trigger media server refresh for all paths where this item appeared
            if updater and updater.initialized:
                for refresh_path in refresh_paths:
                    updater.refresh_path(refresh_path)
                    logger.debug(f"Triggered media server refresh for {refresh_path}")

    logger.info(f"Successfully removed items: {removed_ids}")

    return RemoveResponse(
        message=f"Removed items with ids {removed_ids}",
        ids=removed_ids,
    )


class StreamsResponse(MessageResponse):
    streams: Annotated[
        list[dict[str, Any]],
        Field(description="The list of streams"),
    ]
    blacklisted_streams: Annotated[
        list[dict[str, Any]],
        Field(description="The list of blacklisted streams"),
    ]


@router.get(
    "/{item_id}/streams",
    summary="Get Media Item Streams",
    description="Get streams for a media item",
    operation_id="get_item_streams",
    response_model=StreamsResponse,
)
async def get_item_streams(
    item_id: Annotated[
        int,
        Path(description="The ID of the media item", ge=1),
    ],
) -> StreamsResponse:
    with db_session() as session:
        item = (
            session.execute(select(MediaItem).where(MediaItem.id == item_id))
            .unique()
            .scalar_one_or_none()
        )

    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Item not found"
        )

    return StreamsResponse(
        message=f"Retrieved streams for item {item_id}",
        streams=[stream.to_dict() for stream in item.streams],
        blacklisted_streams=[stream.to_dict() for stream in item.blacklisted_streams],
    )


@router.post(
    "/{item_id}/blocklist_active",
    summary="Blocklist Active Infohash",
    description=(
        "Add the item's currently-active stream infohash to the GLOBAL blocklist "
        "(filesystem.excluded_items.infohashes -> the scraper skips it everywhere, "
        "and it shows on the Blocklist page), blacklist it for this item, and reset "
        "so the item re-scrapes a different release. Use to ditch a bad pick "
        "(wrong audio/language) without blocklisting the whole title."
    ),
    operation_id="blocklist_active_infohash",
    response_model=MessageResponse,
)
async def blocklist_active_infohash(
    item_id: Annotated[
        int,
        Path(description="The ID of the media item", ge=1),
    ],
) -> MessageResponse:
    with db_session() as session:
        item = (
            session.execute(select(MediaItem).where(MediaItem.id == item_id))
            .unique()
            .scalar_one_or_none()
        )

        if not item:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Item not found",
            )

        active = item.active_stream
        infohash = getattr(active, "infohash", None) if active else None

        if not infohash:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Item has no active stream / infohash to blocklist",
            )

        # Add to the global infohash blocklist. The scraper reads this set live
        # (scrapers/shared.py skips any infohash in excluded_items.infohashes),
        # so the mutation takes effect immediately; save() persists it to disk.
        excluded = settings_manager.settings.filesystem.excluded_items
        excluded.infohashes.add(infohash)
        # Capture a human label NOW (item is in scope; after reset the Stream row
        # may be pruned, making infohash->media unrecoverable). Display-only on the
        # Blocklist page; the scraper ignores infohash_labels.
        label = getattr(item, "log_string", None) or getattr(item, "title", None)
        if label:
            excluded.infohash_labels[infohash] = str(label)
        settings_manager.save()

        def mutation(i: MediaItem, s: Session):
            # Mirror /reset: drop the current pick + reset so it re-scrapes.
            # The blocklisted infohash is now skipped, so it can't be re-grabbed.
            i.blacklist_active_stream()
            if isinstance(i, (Show, Season)):
                _reset_scrape_state(i)
            else:
                i.reset()

        apply_item_mutation(
            di[Program],
            session,
            item,
            mutation,
            bubble_parents=True,
        )

        session.commit()

    return MessageResponse(
        message=f"Blocklisted infohash {infohash} for item {item_id}; re-scraping.",
    )


@router.post(
    "/{item_id}/streams/{stream_id}/blacklist",
    summary="Blacklist Media Item Stream",
    description="Blacklist a stream for a media item",
    operation_id="blacklist_item_stream",
    response_model=MessageResponse,
)
async def blacklist_stream(
    item_id: Annotated[
        int,
        Path(
            description="The ID of the media item",
            ge=1,
        ),
    ],
    stream_id: Annotated[
        int,
        Path(
            description="The ID of the stream",
            ge=1,
        ),
    ],
) -> MessageResponse:
    with db_session() as session:
        item = (
            session.execute(select(MediaItem).where(MediaItem.id == item_id))
            .unique()
            .scalar_one_or_none()
        )

        if not item:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Item not found",
            )

        stream = next(
            (stream for stream in item.streams if stream.id == stream_id), None
        )

        if not stream:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Stream not found",
            )

        def mutation(i: MediaItem, s: Session):
            i.blacklist_stream(stream)

        apply_item_mutation(
            di[Program],
            session,
            item,
            mutation,
            bubble_parents=True,
        )

        session.commit()

        return MessageResponse(
            message=f"Blacklisted stream {stream_id} for item {item_id}",
        )


@router.post(
    "/{item_id}/streams/{stream_id}/unblacklist",
    summary="Unblacklist Media Item Stream",
    description="Unblacklist a stream for a media item",
    operation_id="unblacklist_item_stream",
    response_model=MessageResponse,
)
async def unblacklist_stream(
    item_id: Annotated[
        int,
        Path(
            description="The ID of the media item",
            ge=1,
        ),
    ],
    stream_id: Annotated[
        int,
        Path(
            description="The ID of the stream",
            ge=1,
        ),
    ],
) -> MessageResponse:
    with db_session() as db:
        item = (
            db.execute(select(MediaItem).where(MediaItem.id == item_id))
            .unique()
            .scalar_one_or_none()
        )

        if not item:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Item not found"
            )

        stream = next(
            (stream for stream in item.blacklisted_streams if stream.id == stream_id),
            None,
        )

        if not stream:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Stream not found"
            )

        def mutation(i: MediaItem, s: Session):
            i.unblacklist_stream(stream)

        apply_item_mutation(di[Program], db, item, mutation, bubble_parents=True)

        db.commit()

        return MessageResponse(
            message=f"Unblacklisted stream {stream_id} for item {item_id}",
        )


@router.post(
    path="/{item_id}/streams/reset",
    summary="Reset Media Item Streams",
    description="Reset all streams for a media item",
    operation_id="reset_item_streams",
    response_model=MessageResponse,
)
async def reset_item_streams(
    item_id: Annotated[
        int,
        Path(
            description="The ID of the media item",
            ge=1,
        ),
    ],
) -> MessageResponse:
    with db_session() as session:
        item = (
            session.execute(select(MediaItem).where(MediaItem.id == item_id))
            .unique()
            .scalar_one_or_none()
        )

        if not item:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Item not found"
            )

        def mutation(i: MediaItem, s: Session):
            i.streams.clear()
            i.blacklisted_streams.clear()
            i.active_stream = None

        apply_item_mutation(
            di[Program],
            session,
            item,
            mutation,
            bubble_parents=True,
        )

        session.commit()

        return MessageResponse(
            message=f"Successfully reset streams for item {item_id}",
        )


class PauseResponse(MessageResponse):
    ids: Annotated[
        list[int],
        Field(description="The IDs to pause", min_length=1),
    ]


@router.post(
    "/pause",
    summary="Pause Media Items",
    description="Pause media items based on item IDs",
    operation_id="pause_items",
    response_model=PauseResponse,
)
async def pause_items(
    payload: Annotated[
        IdListPayload,
        Body(description="Pause items payload"),
    ],
) -> PauseResponse:
    """Pause items and their children from being processed"""

    parsed_ids = handle_ids(payload.ids)

    try:
        with db_session() as session:
            # Load items using ORM
            items = (
                session.execute(select(MediaItem).where(MediaItem.id.in_(parsed_ids)))
                .scalars()
                .all()
            )

            for media_item in items:
                try:
                    item_id, related_ids = db_functions.get_item_ids(
                        session, media_item.id
                    )
                    all_ids = [item_id] + related_ids

                    # Cancel all related jobs
                    for id in all_ids:
                        di[Program].em.cancel_job(id)
                        di[Program].em.remove_id_from_queues(id)

                    if media_item.last_state not in [
                        States.Paused,
                        States.Failed,
                        States.Completed,
                    ]:

                        def mutation(i: MediaItem, s: Session):
                            i.store_state(States.Paused)

                        apply_item_mutation(
                            di[Program],
                            session,
                            media_item,
                            mutation,
                            bubble_parents=False,
                        )
                        session.commit()

                    logger.info("Successfully paused items.")
                except Exception as e:
                    logger.error(f"Failed to pause {media_item.log_string}: {str(e)}")
                    continue
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return PauseResponse(
        message="Successfully paused items.",
        ids=parsed_ids,
    )


@router.post(
    "/unpause",
    summary="Unpause Media Items",
    description="Unpause media items based on item IDs",
    operation_id="unpause_items",
    response_model=PauseResponse,
)
async def unpause_items(
    payload: Annotated[
        IdListPayload,
        Body(description="Unpause items payload"),
    ],
) -> PauseResponse:
    """Unpause items and their children to resume processing"""

    parsed_ids = handle_ids(payload.ids)

    try:
        with db_session() as session:
            # Load items using ORM
            items = (
                session.execute(select(MediaItem).where(MediaItem.id.in_(parsed_ids)))
                .scalars()
                .all()
            )

            for media_item in items:
                try:
                    if media_item.last_state == States.Paused:

                        def mutation(i: MediaItem, s: Session):
                            i.store_state(States.Requested)

                        apply_item_mutation(
                            di[Program],
                            session,
                            media_item,
                            mutation,
                            bubble_parents=True,
                        )

                        session.commit()

                        di[Program].em.add_event(Event("RetryItem", media_item.id))

                        logger.info(f"Successfully unpaused {media_item.log_string}")
                    else:
                        logger.debug(
                            f"Skipping unpause for {media_item.log_string} - not in paused state"
                        )
                except Exception as e:
                    logger.error(f"Failed to unpause {media_item.log_string}: {str(e)}")
                    continue
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return PauseResponse(
        message="Successfully unpaused items.",
        ids=parsed_ids,
    )


class ReindexPayload(BaseModel):
    item_id: Annotated[
        int | None,
        Field(
            default=None,
            description="The ID of the media item",
        ),
    ]
    tvdb_id: Annotated[
        str | None,
        Field(
            default=None,
            description="The TVDB ID of the media item",
        ),
    ]
    tmdb_id: Annotated[
        str | None,
        Field(
            default=None,
            description="The TMDB ID of the media item",
        ),
    ]
    imdb_id: Annotated[
        str | None,
        Field(
            default=None,
            description="The IMDB ID of the media item",
        ),
    ]

    @model_validator(mode="after")
    def check_at_least_one_id_provided(self) -> Self:
        if not any([self.item_id, self.tvdb_id, self.tmdb_id, self.imdb_id]):
            raise ValueError("At least one ID must be provided")

        return self


@router.post(
    path="/reindex",
    summary="Reindex item to pick up new season & episode releases.",
    description="""
        Submits an item to be re-indexed through the indexer to manually fix shows that don't have release dates.
        Only works for movies and shows. Requires item id as a parameter.
    """,
    operation_id="composite_reindexer",
    response_model=MessageResponse,
)
async def reindex_item(
    payload: Annotated[
        ReindexPayload,
        Body(description="Reindex item payload"),
    ],
) -> MessageResponse:
    """Reindex item through Composite Indexer manually"""

    with db_session() as session:
        # Load item using ORM based on provided ID
        item: MediaItem | None = None

        if payload.item_id:
            item = session.get(MediaItem, payload.item_id)
        elif payload.tvdb_id:
            item = session.execute(
                select(MediaItem).where(MediaItem.tvdb_id == payload.tvdb_id)
            ).scalar_one_or_none()
        elif payload.tmdb_id:
            item = session.execute(
                select(MediaItem).where(MediaItem.tmdb_id == payload.tmdb_id)
            ).scalar_one_or_none()
        elif payload.imdb_id:
            item = session.execute(
                select(MediaItem).where(MediaItem.imdb_id == payload.imdb_id)
            ).scalar_one_or_none()

        if not item:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Item not found"
            )

        if not isinstance(item, Movie | Show):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Item must be a movie or show",
            )

        try:
            services = di[Program].services

            assert services, "Services not initialized"

            indexer_service = services.indexer

            def mutation(i: MediaItem, s: Session):
                # Reset indexed_at to trigger reindexing
                i.indexed_at = None

                # Run the indexer within the session context
                runner_result = next(indexer_service.run(i, log_msg=True))

                if not runner_result.media_items:
                    raise ValueError(
                        "Failed to reindex item - no data returned from indexer"
                    )

                # Merge the reindexed item back into the session
                # Use no_autoflush to prevent SQLAlchemy from trying to flush
                # the new Season/Episode objects before the merge is complete
                with s.no_autoflush:
                    merged = s.merge(runner_result.media_items[0])

                # SQLAlchemy 2.0 does NOT auto-cascade a transient child appended
                # to a persistent parent's collection, so the newly-aired
                # Season/Episode objects the TVDB indexer just created via
                # show.add_season()/season.add_episode() are silently dropped on
                # flush ("Object of type <Season> not in session, add operation
                # along 'Show.seasons' will not proceed"). Without this the
                # reindex endpoint returns 200 "Successfully re-indexed" but never
                # actually adds a new season (or episodes into an existing empty
                # season shell). Add the children explicitly so they persist.
                # Same fix as patch 0031's scrape/auto path (patch 0032).
                if isinstance(merged, Show):
                    for season in merged.seasons:
                        s.add(season)
                        for episode in season.episodes:
                            s.add(episode)

            apply_item_mutation(
                program=di[Program],
                session=session,
                item=item,
                mutation_fn=mutation,
                bubble_parents=True,
            )

            session.commit()

            logger.info(f"Successfully re-indexed {item.log_string}")

            di[Program].em.add_event(Event("RetryItem", item.id))

            return MessageResponse(message=f"Successfully re-indexed {item.log_string}")
        except Exception as e:
            logger.error(f"Failed to re-index {item.log_string}: {str(e)}")

            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to re-index item: {str(e)}",
            )


class ItemAliasesResponse(BaseModel):
    aliases: Annotated[
        dict[str, list[str]] | None,
        Field(description="The item aliases"),
    ]


@router.get(
    "/{item_id}/aliases",
    summary="Get Media Item Aliases",
    description="Get aliases for a media item",
    operation_id="get_item_aliases",
    response_model=ItemAliasesResponse,
)
async def get_item_aliases(
    item_id: Annotated[
        int,
        Path(
            description="The ID of the media item",
            ge=1,
        ),
    ],
) -> ItemAliasesResponse:
    """Get aliases for a media item"""

    with db_session() as session:
        item = (
            session.execute(select(MediaItem).where(MediaItem.id == item_id))
            .unique()
            .scalar_one_or_none()
        )

    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Item not found"
        )

    return ItemAliasesResponse(aliases=item.aliases)


@router.get(
    "/{item_id}/metadata",
    summary="Get Media Item Metadata",
    description="Get metadata for a media item using item ID",
    operation_id="get_item_metadata",
    response_model=MediaMetadata,
)
async def get_item_metadata(
    item_id: Annotated[
        int,
        Path(
            description="The ID of the media item",
            ge=1,
        ),
    ],
) -> MediaMetadata:
    """Get all metadata for a media item using item ID"""

    with db_session() as session:
        item = (
            session.execute(select(MediaItem).where(MediaItem.id == item_id))
            .unique()
            .scalar_one_or_none()
        )

        if not item:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Item not found"
            )

        media_entry = item.media_entry

        if not media_entry or not media_entry.media_metadata:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No metadata available for this item",
            )

        return media_entry.media_metadata
