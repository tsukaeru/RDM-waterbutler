import asyncio
import functools
from urllib import parse
from typing import List, Tuple, Union

from aiohttp.client import patch

from waterbutler.core import provider, streams
from waterbutler.core.path import WaterButlerPath, WaterButlerPathPart
from waterbutler.core import exceptions

from waterbutler.providers.rushfiles import settings as pd_settings
from waterbutler.providers.rushfiles.metadata import (RushFilesRevision,
                                                        BaseRushFilesMetadata,
                                                        RushFilesFileMetadata,
                                                        RushFilesFolderMetadata,
                                                        RushFilesFileRevisionMetadata, )


class RushFilesPathPart(WaterButlerPathPart):
    #TODO Check decoding/encoding function
    DECODE = parse.unquote
    ENCODE = functools.partial(parse.quote, safe='')  # type: ignore
    #TODO Override other properties and methods if necessary

class RushFilesPath(WaterButlerPath):
    PART_CLASS = RushFilesPathPart
    #TODO Override other properties and methods if necessary

#TODO Implement file handling methods
#TODO Check type of parameters and adjust method declaration when appropriate. (WaterButlerPath -> RushFilesPath)
class RushFilesProvider(provider.BaseProvider):
    """Provider for RushFiles cloud storage service.
    """
    NAME = 'rushfiles'
    # BASE_URL = pd_settings.BASE_URL
    BASE_URL = "https://clientgateway.rushfiles.tsukaeru.team/api/shares/"

    def __init__(self, auth: dict, credentials: dict, settings: dict) -> None:
        super().__init__(auth, credentials, settings)
        #TODO Match with RDM-osf.io/addons/rushfiles/models.py:RushFilesProvider::serialize_waterbutler_*
        self.token = self.credentials['token']
        self.share = self.settings['share']
        # print("auth: ",end="")
        # print(auth)
        # print("credentials: ",end="")
        # print(credentials)
        # print("settings: ",end="")
        # print(settings)

    async def validate_v1_path(self, path: str, **kwargs) -> RushFilesPath:
        if path == '/':
            return RushFilesPath('/', _ids=[None], folder=True)
        
        is_folder = path.endswith('/')
        children_path_list = path.lstrip('/').split('/')
        parent_inter_id = self.share['id']

        # next_child_search
        for i, child in enumerate(children_path_list):
            if i != len(children_path_list) - 1:
                response = await self.make_request(
                    'get',
                    self.build_url(str(self.share['id']), 'virtualfiles', str(parent_inter_id), 'children'),
                    expects=(200, 404,),
                    throws=exceptions.MetadataError,
                )
                res = await response.json()
                if response.status == 404:
                    raise exceptions.NotFoundError(path)

                for data in res['Data']:
                    if child == data['PublicName']:
                        parent_inter_id = data['InternalName']
                        break
                    if data == res['Data'][-1]:
                        raise exceptions.NotFoundError(path)
            else:
                response = await self.make_request(
                    'get',
                    self.build_url(str(self.share['id']), 'virtualfiles', str(parent_inter_id)),
                    expects=(200, 404,),
                    throws=exceptions.MetadataError,
                )
                res = await response.json()
                if response.status == 404 or res['Data']['IsFile'] == is_folder:
                    raise exceptions.NotFoundError(path)

        return RushFilesPath(path, prepend=is_folder)

    async def validate_path(self, path: str, **kwargs) -> RushFilesPath:
        if path == '/':
            return RushFilesPath('/', _ids=[None], folder=True)
        
        is_folder = False
        children_path_list = path.lstrip('/').split('/')
        parent_inter_id = self.share['id']

        for i, child in enumerate(children_path_list):
            if i != len(children_path_list) - 1:
                response = await self.make_request(
                    'get',
                    self.build_url(str(self.share['id']), 'virtualfiles', str(parent_inter_id), 'children'),
                    expects=(200, 404,),
                    throws=exceptions.MetadataError,
                )
                res = await response.json()
                if response.status == 404:
                    raise exceptions.NotFoundError(path)

                for data in res['Data']:
                    if child == data['PublicName']:
                        parent_inter_id = data['InternalName']
                        break
                    if data == res['Data'][-1]:
                        raise exceptions.NotFoundError(path)
            else:
                response = await self.make_request(
                    'get',
                    self.build_url(str(self.share['id']), 'virtualfiles', str(parent_inter_id)),
                    expects=(200, 404,),
                    throws=exceptions.MetadataError,
                )
                res = await response.json()
                if response.status == 404:
                    raise exceptions.NotFoundError(path)
                
                is_folder = not res['Data']['IsFile']
                    
        return RushFilesPath(path, prepend=is_folder)

    async def revalidate_path(self,
                              base: WaterButlerPath,
                              name: str,
                              folder: bool=None) -> WaterButlerPath:
        raise NotImplementedError # Or user super if appropriate

    def can_duplicate_names(self) -> bool:
        return False

    @property
    def default_headers(self) -> dict:
        return {'authorization': 'Bearer {}'.format(self.token)}

    def can_intra_move(self, other: provider.BaseProvider, path: WaterButlerPath=None) -> bool:
        #TODO check if really possible. Adjust accordingly
        return self == other

    def can_intra_copy(self, other: provider.BaseProvider, path=None) -> bool:
        #TODO check if really possible. Adjust accordingly
        return self == other

    async def intra_move(self,  # type: ignore
                         dest_provider: provider.BaseProvider,
                         src_path: WaterButlerPath,
                         dest_path: WaterButlerPath) -> Tuple[BaseRushFilesMetadata, bool]:
        #TODO remove if can_intra_move is always false.
        # Check parent implementation and see if it's optimal.
        # Implement better solution if not, remove override completely if it is.
        raise NotImplementedError

    async def intra_copy(self,
                         dest_provider: provider.BaseProvider,
                         src_path: WaterButlerPath,
                         dest_path: WaterButlerPath) -> Tuple[RushFilesFileMetadata, bool]:
        #TODO remove if can_intra_copy is always false
        raise NotImplementedError

    async def download(self,  # type: ignore
                       path: RushFilesPath,
                       revision: str=None,
                       range: Tuple[int, int]=None,
                       **kwargs) -> streams.BaseStream:
        raise NotImplementedError

    async def upload(self,
                     stream,
                     path: WaterButlerPath,
                     *args,
                     **kwargs) -> Tuple[RushFilesFileMetadata, bool]:
        raise NotImplementedError

    async def delete(self,  # type: ignore
                     path: RushFilesPath,
                     confirm_delete: int=0,
                     **kwargs) -> None:
        raise NotImplementedError

    async def metadata(self,  # type: ignore
                       path: RushFilesPath,
                       raw: bool=False,
                       revision=None,
                       **kwargs) -> Union[dict, BaseRushFilesMetadata,
                                          List[Union[BaseRushFilesMetadata, dict]]]:
        # if path.identifier is None:
        #     raise exceptions.MetadataError('{} not found'.format(str(path)), code=404)

        if path.is_dir:
            return await self._get_file_meta(path)
        else:
            pass
            # return await self._file_metadata(path, revision=revision, raw=raw)
    async def _get_file_meta(self, path: WaterButlerPath, raw: bool=False,
                             revision: str=None) -> Union[dict, RushFilesFileMetadata]:
        raise NotImplementedError 

    async def revisions(self, path: RushFilesPath,  # type: ignore
                        **kwargs) -> List[RushFilesRevision]:
        # Probably https://clientgateway.rushfiles.com/swagger/ui/index#!/VirtualFile/VirtualFile_GetVirtualFileHistory
        raise NotImplementedError

    async def create_folder(self,
                            path: WaterButlerPath,
                            folder_precheck: bool=True,
                            **kwargs) -> RushFilesFolderMetadata:
        raise NotImplementedError

    def path_from_metadata(self, parent_path, metadata) -> WaterButlerPath:
        #TODO Check parent implementation and see if it works.
        # Fix if not, remove override completely if it does.
        return super().path_from_metadata(parent_path, metadata)
    
    async def zip(self, path: WaterButlerPath, **kwargs) -> asyncio.StreamReader:
        #TODO RushFiles allows downloading entire folders from web client
        # so probably there is also a way to to this with the API.
        # I will check and if there is, it may be more efficient then default behaviour.
        return super().zip(path, kwargs)

    
