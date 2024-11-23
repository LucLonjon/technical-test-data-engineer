from typing import List
import requests

from moovitamix_etl.extract.dtos.track_dto import TrackDto
from moovitamix_etl.extract.dtos.user_dto import UserDto
from moovitamix_etl.extract.dtos.listen_history import ListenHistoryDto




class Extractor:

    def __init__(self, api_url: str = "http://127.0.0.1:8000"):
        self.base_url = api_url.rstrip("/")
        self.session = requests.Session()

    def _get_request(self, endpoint, size, page) -> List[dict]:
        url = f"{self.base_url}/{endpoint.lstrip('/')}?page={page}&size={size}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()
    
    def get_tracks(self, size=100, page = 1) -> List[TrackDto]:
        """Get all tracks for a given range

        Args:
            size (int, optional): max number of documents to return. Defaults to 100.
            page (int, optional): the page to retrieve the data from. Defaults to 1.

        Returns:
            List[TrackDto]: returns the list of tracks as Data Transfer Objects
        """
        data = self._get_request('/tracks', size, page)
        return [TrackDto.from_dict(track_data) for track_data in data['items']]
    
    
    def get_users(self, size=100, page = 1) -> List[UserDto]:
        """Get all users for a given range

        Args:
            size (int, optional): max number of documents to return. Defaults to 100.
            page (int, optional): the page to retrieve the data from. Defaults to 1.

        Returns:
            List[UserDto]: returns the list of users as Data Transfer Objects
        """
        data = self._get_request('/users', size, page)
        return [UserDto.from_dict(user_data) for user_data in data['items']]
    
    def get_listen_histories(self, size=100, page = 1) -> List[ListenHistoryDto]:
        """Get all listen histories for a given range

        Args:
            size (int, optional): max number of documents to return. Defaults to 100.
            page (int, optional): the page to retrieve the data from. Defaults to 1.

        Returns:
            List[ListenHistory]: returns the list of listen histories as Data Transfer Objects
        """
        data = self._get_request('/listen_history', size, page)
        return [ListenHistoryDto.from_dict(listen_history) for listen_history in data['items']]
    
    
    def get_all_resources(self, page = 1, size = 100) -> tuple[List[TrackDto] , List[UserDto], List[ListenHistoryDto]]:
        """Retrieve all resources in the same method

        Args:
            page (int, optional): the page number to fetch. Defaults to 1.
            size (int, optional): the number of documents to fetch. Defaults to 100.

        Returns:
            tuple[List[TrackDto] , List[UserDto], List[ListenHistoryDto]]: Every resources we can fetch from the API as a tuple
        """
        return self.get_tracks(), self.get_users(), self.get_listen_histories()