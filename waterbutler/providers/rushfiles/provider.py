import json
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

    async def validate_v1_path(self, path: str, **kwargs) -> RushFilesPath:
        rf_path = await self.validate_path(path, **kwargs)

        if not rf_path.identifier:
            raise exceptions.NotFoundError(str(rf_path))

        return rf_path

    async def validate_path(self, path: str, **kwargs) -> RushFilesPath:
        if path == '/':
            return RushFilesPath('/', _ids=[self.share['id']], folder=True)
        
        is_folder = path.endswith('/')
        children_path_list = path.strip('/').split('/')
        inter_id_list = [self.share['id']]
        current_inter_id = self.share['id']

        for i, child in enumerate(children_path_list):
            response = await self.make_request(
                'GET',
                self.build_url(str(self.share['id']), 'virtualfiles', str(current_inter_id), 'children'),
                expects=(200, 404,),
                throws=exceptions.MetadataError,
            )
            if response.status == 404:
                raise exceptions.NotFoundError(path)
            res = await response.json()
            current_inter_id, index = self._search_inter_id(res, child)
            inter_id_list.append(current_inter_id)
            if not current_inter_id:
                if i == len(children_path_list)-1:
                    return RushFilesPath(path,  _ids=inter_id_list)
                raise exceptions.NotFoundError(path)
            
        if res['Data'][index]['IsFile'] == is_folder:
            raise exceptions.NotFoundError(path)

        return RushFilesPath(path, folder= is_folder, _ids=inter_id_list)                    

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
        if not path.identifier:
            raise exceptions.NotFoundError(str(path))
        if path.is_root:
            raise exceptions.DeleteError(
                'root cannot be deleted',
                code=400
            )

        metadata = await self.metadata(path)
        if not metadata.extra['deleted']:
            raise exceptions.DeleteError(
                'Delete permission required',
                code=403
            )
        response = await self.make_request(
                        'DELETE',
                        self.build_url(str(self.share['id']), 'files', metadata.extra['internalName']),
                        data=json.dumps({
                            "TransmitId": "1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A",
                            "ClientJournalEventType": 1,
                            "DeviceId": "waterbutler "
                        }),
                        expects=(200, 404,),
                        throws=exceptions.MetadataError,
                    )
        if response.status == 404:
            raise exceptions.NotFoundError(str(path))
        
        return

    async def metadata(self,  # type: ignore
                       path: RushFilesPath,
                       raw: bool=False,
                       revision=None,
                       **kwargs) -> Union[dict, BaseRushFilesMetadata,
                                          List[Union[BaseRushFilesMetadata, dict]]]:
        if path.identifier is None:
            raise exceptions.MetadataError('{} not found'.format(str(path)), code=404)

        if path.is_dir:
            return await self._folder_metadata(path, raw=raw)

        return await self._file_metadata(path, raw=raw)

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

    async def _folder_metadata(self,
                               path: RushFilesPath,
                               raw: bool=False) -> List[Union[BaseRushFilesMetadata, dict]]:
        share_id = self.share['id']
        inter_id =  path.identifier

        response = await self.make_request(
            'GET',
            self.build_url(str(share_id), 'virtualfiles', inter_id, 'children'),
            expects=(200, 404,),
            throws=exceptions.MetadataError,
        )

        if response.status == 404:
            raise exceptions.NotFoundError(path)
        res = await response.json()

        if raw:
            return res['Data']
        else:
            ret = []
            for data in res['Data']:
                if data['IsFile']:
                    ret.append(RushFilesFileMetadata(data, path))
                else:
                    ret.append(RushFilesFolderMetadata(data, path))
            return ret

    async def _file_metadata(self,
                             path: RushFilesPath,
                             revision: str=None,
                             raw: bool=False) -> Union[dict, BaseRushFilesMetadata]:
        response = await self.make_request(
            'GET',
            self.build_url(str(self.share['id']), 'virtualfiles', path.identifier),
            expects=(200, 404,),
            throws=exceptions.MetadataError,
        )

        if response.status == 404:
            raise exceptions.NotFoundError(path)

        res = await response.json()

        return res['Data'] if raw else RushFilesFileMetadata(res['Data'], path)

    def _search_inter_id(self, 
                        res: dict, 
                        child: str) -> Union[str,int,None]:
        for i, data in enumerate(res['Data']):
            if child == data['PublicName']:
                return data['InternalName'], i
        return None, None
