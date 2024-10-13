from airflow.plugins_manager import AirflowPlugin
from Operators.SpotifyApiOperator import SpotifyApiOperator



class SpotifyApiOperator(AirflowPlugin):
    name = "spotify_api_plugin"
    operators = [SpotifyApiOperator]