import typing
from waterbutler.core import metadata


class BaseRushFilesMetadata(metadata.BaseMetadata):

    def __init__(self, raw, path):
        super().__init__(raw)
        self._path = path

    @property
    def provider(self):
        return 'rushfiles'


class RushFilesFolderMetadata(BaseRushFilesMetadata, metadata.BaseFolderMetadata):
    def __init__(self, raw, path):
        super().__init__(raw, path)
        self._path._is_folder = True

    @property
    def name(self) -> str:
        return self.raw['Data']['PublicName']

    @property
    def internal_name(self) -> str:
        return self.raw['Data']['InternalName']

    @property
    def share_id(self) -> str:
        return self.raw['Data']['ShareId']
    
    @property
    def parent_id(self) -> str:
        return self.raw['Data']['ParrentId']
        
    @property
    def deleted(self) -> bool:
        return self.raw['Data']['Deleted']
    
    @property
    def path(self) -> str:
        return '/' + self._path.raw_path


class RushFilesFileMetadata(BaseRushFilesMetadata, metadata.BaseFileMetadata):
    @property
    def name(self) -> str:
        return self.raw['Data']['PublicName']

    @property
    def path(self) -> str:
        return '/' + self._path.raw_path

    @property
    def size(self) -> typing.Union[int, str]:
        raise NotImplementedError

    @property
    def modified(self) -> str:
        return self.raw['Data']['LastWriteTime']

    @property
    def created_utc(self) -> str:
        return self.raw['Data']['CreationTime']

    @property
    def content_type(self) -> typing.Union[str, None]:
        if self.raw['Data']['IsFile']:
            return 'file'
        else:
            return 'folder'

    @property
    def etag(self) -> typing.Union[str, None]:
        #TODO Can we return something? Remove if not
        raise NotImplementedError



# TODO Remove if not necessary
class RushFilesFileRevisionMetadata(RushFilesFileMetadata):
    pass


class RushFilesRevision(metadata.BaseFileRevisionMetadata):

    @property
    def version_identifier(self):
        raise NotImplementedError

    @property
    def version(self):
        raise NotImplementedError

    @property
    def modified(self):
        raise NotImplementedError
