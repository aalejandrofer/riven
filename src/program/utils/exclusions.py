from typing import TYPE_CHECKING

from program.media.item import Movie, Show
from program.settings import settings_manager

if TYPE_CHECKING:
    from program.media.item import MediaItem


class Exclusions:
    excluded_shows: set[str]
    excluded_movies: set[str]

    def __init__(self):
        excluded_items = settings_manager.settings.filesystem.excluded_items

        self.excluded_movies = excluded_items.movies
        self.excluded_shows = excluded_items.shows

    def is_excluded(self, item: "MediaItem") -> bool:
        # In v1.0.0, only Show/Season/Episode define top_parent; Movie
        # and the generic base MediaItem do not. Use hasattr to be
        # safe: anything without a top_parent is not part of a show
        # tree we can check exclusions against (e.g. type="mediaitem"
        # placeholder rows).
        if isinstance(item, Movie):
            return self._is_excluded_movie(item)
        if hasattr(item, "top_parent"):
            return self._is_excluded_show(item.top_parent)
        return False

    def _is_excluded_show(self, item: Show) -> bool:
        if item.tvdb_id is None:
            return False

        return str(item.tvdb_id) in self.excluded_shows

    def _is_excluded_movie(self, item: Movie) -> bool:
        if item.tmdb_id is None and item.imdb_id is None:
            return False

        return (
            str(item.tmdb_id) in self.excluded_movies
            or str(item.imdb_id) in self.excluded_movies
        )


exclusions = Exclusions()
