import aiohttp
import mimetypes

from waterbutler.core import streams
from waterbutler.core import provider
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.fedora.metadata import FedoraFileMetadata
from waterbutler.providers.fedora.metadata import FedoraFolderMetadata

# User specifies a Fedora 4 repo which they have access to through HTTP basic authentication.
#
# Credentials:
#   repo:     URL to Fedora 4 repository
#   user:     Username for repo
#   password: Password for repo
#
# Provider written against Fedora 4.5.1
# TODO Add support for revisions


class FedoraProvider(provider.BaseProvider):
    NAME = 'fedora'

    def __init__(self, auth, credentials, prov_settings):
        super().__init__(auth, credentials, prov_settings)
        self.repo = self.credentials['repo']
        self.basic_auth_token = aiohttp.BasicAuth(self.credentials['user'], self.credentials['password']).encode()

    # Return Fedora url for resource identified by WaterButlerPath
    # Must turn WaterButlerPath into list of segments for build_url
    def build_repo_url(self, path, **query):
        segments = [s.original_raw for s in path.parts]
        return provider.build_url(self.repo, *segments, **query)

    # Return WaterButlerPath for a path string.
    # Ensure that if the path is to a folder, it corresponds to a Fedora container
    # and otherwise corresponds to a Fedora binary.
    # Fedora resource must also exist.
    # Throw NotFoundError if resource does not exist or types do not match
    async def validate_v1_path(self, path, **kwargs):
        wb_path = WaterButlerPath(path)
        url = self.build_repo_url(wb_path)

        is_container = await self.is_fedora_container(url)

        if is_container == wb_path.is_dir:
            return wb_path

        raise exceptions.NotFoundError(str(path))

    # Return WaterButlerPath for a path string.
    # Any path understood by WaterButlerPath is fine
    async def validate_path(self, path, **kwargs):
        return WaterButlerPath(path)

    def can_duplicate_names(self):
        return False

    # Copy and move only supported within this provider.

    def can_intra_move(self, other, path=None):
        return self == other

    def can_intra_copy(self, other, path=None):
        return self == other

    # Transform a url in the fedora repo to a WaterButlerPath
    def fedora_url_to_path(self, url):
        return WaterButlerPath('/' + url[len(self.repo):].strip('/'))

    # Copies src_path to dest_path.
    # Returns BaseMetadata, Success tuple.
    async def intra_copy(self, dest_provider, src_path, dest_path):
        src_url = self.build_repo_url(src_path)
        dest_url = self.build_repo_url(dest_path)

        async with self.request(
            'COPY', src_url,
            headers={'Destination': dest_url},
            expects=(201,),
            throws=exceptions.IntraCopyError
        ) as resp:
            # Recalcuate destination path based on Location header
            dest_path = self.fedora_url_to_path(resp.headers.get('Location'))

            md = await self.lookup_fedora_metadata(dest_path)

            return md, True

    # Moves src_path to dest_path.
    # Returns BaseMetadata, Success tuple.
    async def intra_move(self, dest_provider, src_path, dest_path):
        src_url = self.build_repo_url(src_path)
        dest_url = self.build_repo_url(dest_path)

        async with self.request(
            'MOVE', src_url,
            headers={'Destination': dest_url},
            expects=(201,),
            throws=exceptions.IntraMoveError
        ) as move_resp:
            # Delete tombstone of original file
            async with self.request(
                 'DELETE', src_url + '/fcr:tombstone',
                 expects=(204, ),
                 throws=exceptions.DeleteError,
            ):
                pass

            # Recalcuate destination path based on Location header
            dest_path = self.fedora_url_to_path(move_resp.headers.get('Location'))

            md = await self.lookup_fedora_metadata(dest_path)
            return md, True

    @property
    def default_headers(self):
        return {
            'Authorization': self.basic_auth_token
        }

    # Download a Fedora binary
    async def download(self, path, revision=None, range=None, **kwargs):
        url = self.build_repo_url(path)

        resp = await self.make_request(
            'GET',
            url,
            range=range,
            expects=(200,),
            throws=exceptions.DownloadError,
        )

        return streams.ResponseStreamReader(resp)

    # Create a Fedora binary corrsponding to the path and return FedoraFileMetadata for it
    async def upload(self, stream, path, conflict='replace', **kwargs):
        path, exists = await self.handle_name_conflict(path, conflict=conflict)
        url = self.build_repo_url(path)

        # Must provide a Content-Type. Otherwise a container is created.
        mime_type, encoding = mimetypes.guess_type(url)

        if mime_type is None:
            mime_type = 'application/octet-stream'

        # Must not say content is RDF because a container will be created.
        if mime_type in ['text/turtle', 'text/rdf+n3', 'application/n3', 'text/n3'
                            'application/rdf+xml' 'application/n-triples' 'application/ld+json']:
            mime_type = 'application/octet-stream'

        async with self.request(
            'PUT',
            url,
            headers={'Content-Length': str(stream.size), 'Content-Type': mime_type},
            data=stream,
            expects=(201, ),
            throws=exceptions.UploadError,
        ):
            md = await self.metadata(path)
            return md, True

    # Delete the Fedora resource corrsponding to the path
    # Must also delete the tombstone so the resource can be recreated.
    async def delete(self, path, confirm_delete=0, **kwargs):
        url = self.build_repo_url(path)

        async with self.request(
            'DELETE', url,
            expects=(204, ),
            throws=exceptions.DeleteError,
        ):
            pass

        async with self.request(
            'DELETE', url + '/fcr:tombstone',
            expects=(204, ),
            throws=exceptions.DeleteError,
        ):
            pass

    # Given a WaterBulterPath, return metadata about the specified resource.
    # The JSON-LD representations of Fedora resources are parsed as simple JSON.
    # This is a little brittle and may cause issues in the future.
    async def metadata(self, path, revision=None, **kwargs):
        result = await self.lookup_fedora_metadata(path)

        # If fedora resource is container, return list of metadata about child resources.

        if result.is_folder:
            return result.list_children_metadata(self.repo)
        else:
            return result

    # Return FedoraFileMetadata for a Fedora binary or FedoraFolderMetadata for a container.
    # Must do a HEAD request to figure out how to retrieve metadata because the url to a Fedora binary
    # resource must have /fcr:metadata appended to it.
    # The Prefer header tells fedora to include triples for child resources.
    async def lookup_fedora_metadata(self, path):
        fedora_id = self.build_repo_url(path)
        is_container = await self.is_fedora_container(fedora_id)

        if is_container:
            url = fedora_id
        else:
            url = fedora_id + '/fcr:metadata'

        async with self.request(
            'GET', url,
            headers={'Accept': 'application/ld+json',
                     'Prefer': 'return=representation; include="http://fedora.info/definitions/v4/repository#EmbedResources"'},
            expects=(200, 404),
            throws=exceptions.MetadataError
        ) as resp:

            if resp.status == 404:
                raise exceptions.NotFoundError(str(path))

            raw = await resp.json()

            if is_container:
                return FedoraFolderMetadata(raw, fedora_id, path)
            else:
                return FedoraFileMetadata(raw, fedora_id, path)

    # Do a head request on a url to check if it is a fedora container
    async def is_fedora_container(self, url):
        async with self.request(
            'HEAD', url,
            expects=(200, 404),
            throws=exceptions.MetadataError
        ) as resp:
            if resp.status == 404:
                raise exceptions.NotFoundError(str(url))

            return '<http://www.w3.org/ns/ldp#Container>;rel="type"' in resp.headers.getall('Link', [])

    # Create the specified folder as a Fedora container and return FedoraFolderMetadata for it
    async def create_folder(self, path, folder_precheck=True, **kwargs):
        WaterButlerPath.validate_folder(path)

        url = self.build_repo_url(path)

        async with self.request(
            'PUT',
            url,
            expects=(201,),
            throws=exceptions.CreateFolderError
        ):
            pass

        md = await self.lookup_fedora_metadata(path)
        return md