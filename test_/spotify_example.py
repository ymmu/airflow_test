from pprint import pprint
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from src.af_batch.src import utils_

utils_.set_spotify_config()

auth_manager = SpotifyClientCredentials()
sp = spotipy.Spotify(auth_manager=auth_manager)

# playlists = sp.user_playlists('spotify')
# while playlists:
#     for i, playlist in enumerate(playlists['items']):
#         print("%4d %s %s" % (i + 1 + playlists['offset'], playlist['uri'],  playlist['name']))
#     if playlists['next']:
#         playlists = sp.next(playlists)
#     else:
#         playlists = None

name = 'Radiohead'

results = sp.search(q='artist:' + name, type='artist')
items = results['artists']['items']
if len(items) > 0:
    artist = items[0]
    print(artist['name'], artist['images'][0]['url'])

search = sp.search(q='artist:Leisure track:nobody AND Gold link', type='track')
print(len(search['tracks']['items']))
print([i['name'] for i in search['tracks']['items']])
# search = sp.search(q='artist:Marcin track:Kashmir', type='artist,track')
# search = sp.search(q='artist:Marcin', type='artist')
pprint(search)
# urn = 'spotify:artist:2F7PtF4lRVIufJd6Sjud71'
# artist = sp.artist(urn)
# pprint(artist)
# sp.albums()
#user = sp.user('plamere')
#pprint(user)


# test ---
# def parallel_dataframe(df, func):
#     global num_cores
#     num_cores = mp.cpu_count()
#     df_split = np.array_split(df, num_cores)
#     pool = mp.Pool(num_cores)
#     df = pd.concat(pool.map(func, df_split))
#     pool.close()
#     pool.join()
#     return df
