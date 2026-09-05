from typing import TYPE_CHECKING

from program.media.item import Movie, Show
from program.settings import settings_manager

if TYPE_CHECKING:
    from program.media.item import MediaItem


class Exclusions:
    # Patch 0013: read settings dynamically so /settings/load picks up
    # excluded_items changes without a restart.

    @property
    def excluded_shows(self) -> set[str]:
        items = settings_manager.settings.filesystem.excluded_items
        return set(items.shows or [])

    @property
    def excluded_movies(self) -> set[str]:
        items = settings_manager.settings.filesystem.excluded_items
        return set(items.movies or [])

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
        # Patch 0016: shows may lack tvdb_id (e.g. anime, unmatched).
        # Fall back to tmdb_id / imdb_id so exclusion still works.
        ids = [item.tvdb_id, item.tmdb_id, item.imdb_id]
        return any(i is not None and str(i) in self.excluded_shows for i in ids)

    def _is_excluded_movie(self, item: Movie) -> bool:
        if item.tmdb_id is None and item.imdb_id is None:
            return False

        return (
            str(item.tmdb_id) in self.excluded_movies
            or str(item.imdb_id) in self.excluded_movies
        )


exclusions = Exclusions()
