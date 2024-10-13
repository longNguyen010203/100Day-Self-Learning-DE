from typing import Any
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class SpotifyApiOperator(BaseOperator):
    """ 
    Defines target class SpotifyApiOperator in conjunction 
    with Spotify API, inheriting from class BaseOperator.
    
    :param spotify_client_id 
    :param spotify_client_secret
    """
    
    def __init__(self, spotify_client_id: str, spotify_client_secret: str, **kwargs: str) -> None:
        super().__init__(**kwargs)
        self.spotify_client_id = spotify_client_id
        self.spotify_client_secret = spotify_client_secret
        
        
    def spotify_connect(self) -> Spotify:
        return Spotify(
            client_credentials_manager=SpotifyClientCredentials(
                client_id=self.spotify_client_id,
                client_secret=self.spotify_client_secret
            )
        )
        
    
    def search_for_item(self, limit: int, offset: int, type: str, market: str = "ES") -> dict[str, Any]:
        """ 
        Get Spotify catalog information about albums, artists, playlists, tracks, shows, 
        episodes or audiobooks that match a keyword string. Audiobooks are only available 
        within the US, UK, Canada, Ireland, New Zealand and Australia markets.

        :param limit: The maximum number of results to return in each item type, Range: 0 - 50
        :param offset: The index of the first result to return. Use with limit to get the next page of search results, ange: 0 - 1000
        :param type: Allowed values: "album", "artist", "playlist", "track", "show", "episode", "audiobook"
        """
        
        spotify = self.spotify_connect()
        
        return spotify.search(
            q="remaster%20track:Doxy%20artist:Miles%20Davis",
            limit=limit,
            offset=offset,
            type=type,
            market=market
        )
            