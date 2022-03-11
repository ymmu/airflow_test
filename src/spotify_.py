from pprint import pprint
import spotipy, os
from spotipy.oauth2 import SpotifyClientCredentials
import utils_


def get_track_data(artist, track):
    utils_.set_spotify_config()
    auth_manager = SpotifyClientCredentials()
    sp = spotipy.Spotify(auth_manager=auth_manager)

    # search = sp.search(q='artist:Marcin track:Kashmir', type='artist,track')
    search = sp.search(q=f'artist:{artist} track:{track}', type='track')  # 하나씩만 검색가능.
    # search = sp.search(q='artist:Marcin', type='artist')
    pprint(search)
    return search