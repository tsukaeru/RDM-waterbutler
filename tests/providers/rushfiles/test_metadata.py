import pytest

import os
import json

from waterbutler.providers.rushfiles.provider import RushFilesPath
from waterbutler.providers.rushfiles.provider import RushFilesPathPart
from waterbutler.providers.rushfiles.metadata import RushFilesRevision
from waterbutler.providers.rushfiles.metadata import RushFilesFileMetadata
from waterbutler.providers.rushfiles.metadata import RushFilesFolderMetadata

from tests.providers.googledrive.fixtures import(
    root_provider_fixtures,
)
@pytest.fixture
def basepath():
    return RushFilesPath('/conrad')
    
class TestMetadata:

    def test_file_metadata_drive(self, basepath, root_provider_fixtures):
        print(root_provider_fixtures)
        item = root_provider_fixtures['file_metadata']
        path = basepath.child(item['Data']['PublicName'])
        parsed = RushFilesFileMetadata(item, path)

        assert parsed.provider == 'rushfiles'
        assert path.name == item['Data']['PublicName']
        assert parsed.name == item['Data']['PublicName']
        assert parsed.internal_name == item['Data']['InternalName']
        assert parsed.share_id == item['Data']['ShareId']
        assert parsed.parent_id == item['Data']['ParrentId']
        assert parsed.deleted == item['Data']['Deleted']
        assert parsed.modified == item['Data']['LastWriteTime']
        assert parsed.content_type == 'file' if item['Data']['IsFile'] else 'folder'
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts])
    
    def test_folder_metadata(self, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata']
        path = RushFilesPath('/we/love/you/conrad').child(item['Data']['PublicName'], folder=True)
        parsed = RushFilesFolderMetadata(item, path)

        assert parsed.provider == 'rushfiles'
        assert parsed.name == item['Data']['PublicName']
        assert parsed.internal_name == item['Data']['InternalName']
        assert parsed.share_id == item['Data']['ShareId']
        assert parsed.parent_id == item['Data']['ParrentId']
        assert parsed.deleted == item['Data']['Deleted']
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts]) + '/'
