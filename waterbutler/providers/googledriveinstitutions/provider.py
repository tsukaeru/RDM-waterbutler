import os
import json
import hashlib
import functools
from urllib import parse
from http import HTTPStatus
from typing import List, Sequence, Tuple, Union

import furl

from waterbutler.core import exceptions, provider, streams
from waterbutler.core.path import WaterButlerPath, WaterButlerPathPart

from waterbutler.providers.googledriveinstitutions import utils
from waterbutler.providers.googledriveinstitutions import settings as pd_settings
from waterbutler.providers.googledriveinstitutions.metadata import (GoogleDriveInstitutionsRevision,
                                                        BaseGoogleDriveInstitutionsMetadata,
                                                        GoogleDriveInstitutionsFileMetadata,
                                                        GoogleDriveInstitutionsFolderMetadata,
                                                        GoogleDriveInstitutionsFileRevisionMetadata, )


def clean_query(query: str):
    # Replace \ with \\ and ' with \'
    # Note only single quotes need to be escaped
    return query.replace('\\', r'\\').replace("'", r"\'")


class GoogleDriveInstitutionsPathPart(WaterButlerPathPart):
    DECODE = parse.unquote
    # TODO: mypy lacks a syntax to define kwargs for callables
    ENCODE = functools.partial(parse.quote, safe='')  # type: ignore


class GoogleDriveInstitutionsPath(WaterButlerPath):
    PART_CLASS = GoogleDriveInstitutionsPathPart


class GoogleDriveInstitutionsProvider(provider.BaseProvider):
    """Provider for Google's Drive cloud storage service.

    This provider uses the v3 Drive API.

    API docs: https://developers.google.com/drive/v3/reference/

    Quirks:

    * Google doc files (``.gdoc``, ``.gsheet``, ``.gsheet``, ``.gdraw``) cannot be downloaded in
      their native format and must be exported to another format.  e.g. ``.gdoc`` to ``.docx``

    * Some Google doc files (currently ``.gform`` and ``.gmap``) do not have an available export
      format and cannot be downloaded at all.

    * Google Drive is not really a filesystem.  Folders are actually labels, meaning a file ``foo``
      could be in two folders (ex. ``A``, ``B``) at the same time.  Deleting ``/A/foo`` will
      cause ``/B/foo`` to be deleted as well.

    Revisions:

    Both Google Drive and WaterButler have weird behaviors wrt file revisions.  Google docs use a
    simple integer versioning system.  Non-Google doc files, like jpegs or text files, use strings
    that resemble the standard Google Drive file ID format (ex.
    ``0B74RCNS4TbRVTitFais4VzVmQlQ4S0docGlhelk5MXE3OFJnPQ``).  In addition, revision history is not
    available for any file that the user only has view or commenting permissions for.  In the past
    WB forged revision ids for these files by taking the etag of the file and appending a sentinel
    value (set in `googledriveinstitutions.settings.DRIVE_IGNORE_VERSION`) to the end.  If WB receives a request
    to download a file with a revision ending with the sentinel string, we ignore the revision and
    return the latest version instead.  The file metadata endpoint will behave the same.  A metadata
    or download request for a readonly file with a revision value that doesn't end with the sentinel
    value will always return a 404 Not Found.
    """
    NAME = 'googledriveinstitutions'
    BASE_URL = pd_settings.BASE_URL
    FOLDER_MIME_TYPE = 'application/vnd.google-apps.folder'

    def __init__(self, auth: dict, credentials: dict, settings: dict) -> None:
        super().__init__(auth, credentials, settings)
        self.token = self.credentials['token']
        self.folder = self.settings['folder']

    async def validate_v1_path(self, path: str, **kwargs) -> GoogleDriveInstitutionsPath:
        if path == '/':
            return GoogleDriveInstitutionsPath('/', _ids=[self.folder['id']], folder=True)

        implicit_folder = path.endswith('/')
        parts = await self._resolve_path_to_ids(path)
        explicit_folder = parts[-1]['mimeType'] == self.FOLDER_MIME_TYPE
        if parts[-1]['id'] is None or implicit_folder != explicit_folder:
            raise exceptions.NotFoundError(str(path))

        names, ids = zip(*[(parse.quote(x['name'], safe=''), x['id']) for x in parts])
        return GoogleDriveInstitutionsPath('/'.join(names), _ids=ids, folder='folder' in parts[-1]['mimeType'])

    async def validate_path(self, path: str, **kwargs) -> GoogleDriveInstitutionsPath:
        if path == '/':
            return GoogleDriveInstitutionsPath('/', _ids=[self.folder['id']], folder=True)

        parts = await self._resolve_path_to_ids(path)
        names, ids = zip(*[(parse.quote(x['name'], safe=''), x['id']) for x in parts])
        return GoogleDriveInstitutionsPath('/'.join(names), _ids=ids, folder='folder' in parts[-1]['mimeType'])

    async def revalidate_path(self,
                              base: WaterButlerPath,
                              name: str,
                              folder: bool=None) -> WaterButlerPath:
        # TODO Redo the logic here folders names ending in /s
        # Will probably break
        if '/' in name.lstrip('/') and '%' not in name:
            # DAZ and MnC may pass unquoted names which break
            # if the name contains a / in it
            name = parse.quote(name.lstrip('/'), safe='')

        if not name.endswith('/') and folder:
            name += '/'

        parts = await self._resolve_path_to_ids(name, start_at=[{
            'name': base.name,
            'mimeType': 'folder',
            'id': base.identifier,
        }])
        _id, name, mime = list(map(parts[-1].__getitem__, ('id', 'name', 'mimeType')))
        return base.child(name, _id=_id, folder='folder' in mime)

    def can_duplicate_names(self) -> bool:
        return True

    @property
    def default_headers(self) -> dict:
        return {'authorization': 'Bearer {}'.format(self.token)}

    def can_intra_move(self, other: provider.BaseProvider, path: WaterButlerPath=None) -> bool:
        return self == other

    def can_intra_copy(self, other: provider.BaseProvider, path=None) -> bool:
        # gdrive doesn't support intra-copy on folders
        return self == other and (path and path.is_file)

    async def intra_move(self,  # type: ignore
                         dest_provider: provider.BaseProvider,
                         src_path: WaterButlerPath,
                         dest_path: WaterButlerPath) -> Tuple[BaseGoogleDriveInstitutionsMetadata, bool]:
        self.metrics.add('intra_move.destination_exists', dest_path.identifier is not None)
        if dest_path.identifier:
            await dest_provider.delete(dest_path)

        async with self.request(
            'UPDATE', # 'PATCH',
            self.build_url('files', src_path.identifier),
            headers={
                'Content-Type': 'application/json'
            },
            data=json.dumps({
                'parents': [{
                    'id': dest_path.parent.identifier
                }],
                'name': dest_path.name
            }),
            expects=(200, ),
            throws=exceptions.IntraMoveError,
        ) as resp:
            data = await resp.json()

        created = dest_path.identifier is None
        dest_path.parts[-1]._id = data['id']

        if dest_path.is_dir:
            metadata = GoogleDriveInstitutionsFolderMetadata(data, dest_path)
            metadata._children = await self._folder_metadata(dest_path)
            return metadata, created
        else:
            return GoogleDriveInstitutionsFileMetadata(data, dest_path), created  # type: ignore

    async def intra_copy(self,
                         dest_provider: provider.BaseProvider,
                         src_path: WaterButlerPath,
                         dest_path: WaterButlerPath) -> Tuple[GoogleDriveInstitutionsFileMetadata, bool]:
        self.metrics.add('intra_copy.destination_exists', dest_path.identifier is not None)
        if dest_path.identifier:
            await dest_provider.delete(dest_path)

        async with self.request(
            'POST',
            self.build_url('files', src_path.identifier, 'copy',
                            fields='id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                'originalFilename,md5Checksum,exportLinks,capabilities(canEdit)'),
            headers={'Content-Type': 'application/json'},
            data=json.dumps({
                'parents': [{
                    'id': dest_path.parent.identifier
                }],
                'name': dest_path.name
            }),
            expects=(200, ),
            throws=exceptions.IntraMoveError,
        ) as resp:
            data = await resp.json()

        # GoogleDrive doesn't support intra-copy for folders, so dest_path will always
        # be a file.  See can_intra_copy() for type check.
        return GoogleDriveInstitutionsFileMetadata(data, dest_path), dest_path.identifier is None

    async def download(self,  # type: ignore
                       path: GoogleDriveInstitutionsPath,
                       revision: str=None,
                       range: Tuple[int, int]=None,
                       **kwargs) -> streams.BaseStream:
        """Download the file at `path`.  If `revision` is present, attempt to download that revision
        of the file.  See **Revisions** in the class doctring for an explanation of this provider's
        revision handling.   The actual revision handling is done in `_file_metadata()`.

        Quirks:

        Google docs don't have a size until they're exported, so WB must download them, then
        re-stream them as a StringStream.

        :param GoogleDriveInstitutionsPath path: the file to download
        :param str revision: the id of a particular version to download
        :param tuple(int, int) range: range of bytes to download in this request
        :rtype: streams.ResponseStreamReader
        :rtype: streams.StringStream
        :returns: For GDocs, a StringStream.  All others, a ResponseStreamReader.
        """

        metadata = await self.metadata(path, revision=revision)

        download_resp = await self.make_request(
            'GET',
            self.build_url('files', path.identifier, alt='media') or
                utils.get_export_link(metadata.raw),
            range=range,
            expects=(200, 206),
            throws=exceptions.DownloadError,
        )

        if metadata.size is not None:  # type: ignore
            return streams.ResponseStreamReader(download_resp, size=metadata.size_as_int)  # type: ignore

        # google docs, not drive files, have no way to get the file size
        # must buffer the entire file into memory
        stream = streams.StringStream(await download_resp.read())
        if download_resp.headers.get('Content-Type'):
            # TODO: Add these properties to base class officially, instead of as one-off
            stream.content_type = download_resp.headers['Content-Type']  # type: ignore
        stream.name = metadata.export_name  # type: ignore
        return stream

    async def upload(self,
                     stream,
                     path: WaterButlerPath,
                     *args,
                     **kwargs) -> Tuple[GoogleDriveInstitutionsFileMetadata, bool]:
        assert path.is_file

        if path.identifier:
            segments = [path.identifier]
        else:
            segments = []

        stream.add_writer('md5', streams.HashStreamWriter(hashlib.md5))

        upload_metadata = self._build_upload_metadata(path.parent.identifier, path.name)
        location = await self._start_resumable_upload(not path.identifier, segments, stream.size,
                                                       upload_metadata)
        data = await self._finish_resumable_upload(segments, stream, location)

        if data['md5Checksum'] != stream.writers['md5'].hexdigest:
            raise exceptions.UploadChecksumMismatchError()

        created = path.identifier is None
        path._parts[-1]._id = data.get('id')
        return GoogleDriveInstitutionsFileMetadata(data, path), created

    async def delete(self,  # type: ignore
                     path: GoogleDriveInstitutionsPath,
                     confirm_delete: int=0,
                     **kwargs) -> None:
        """Given a WaterButlerPath, delete that path
        :param GoogleDriveInstitutionsPath path: Path to be deleted
        :param int confirm_delete: Must be 1 to confirm root folder delete
        :rtype: None
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        :raises: :class:`waterbutler.core.exceptions.DeleteError`

        Quirks:
            If the WaterButlerPath given is for the provider root path, then
            the contents of provider root path will be deleted. But not the
            provider root itself.
        """
        if not path.identifier:
            raise exceptions.NotFoundError(str(path))

        self.metrics.add('delete.is_root_delete', path.is_root)
        if path.is_root:
            self.metrics.add('delete.root_delete_confirmed', confirm_delete == 1)
            if confirm_delete == 1:
                await self._delete_folder_contents(path)
                return
            else:
                raise exceptions.DeleteError(
                    'confirm_delete=1 is required for deleting root provider folder',
                    code=400
                )

        async with self.request(
            'DELETE',
            self.build_url('files', path.identifier),
            data=json.dumps({'labels': {'trashed': 'true'}}),
            headers={'Content-Type': 'application/json'},
            expects=(200, ),
            throws=exceptions.DeleteError,
        ):
            return

    def _build_query(self, folder_id: str, name: str=None) -> str:
        queries = [
            "'{}' in parents".format(folder_id),
            'trashed = false',
            "mimeType != 'application/vnd.google-apps.form'",
            "mimeType != 'application/vnd.google-apps.map'",
        ]
        if name:
            queries.append("name = '{}'".format(clean_query(name)))
        return ' and '.join(queries)

    async def metadata(self,  # type: ignore
                       path: GoogleDriveInstitutionsPath,
                       raw: bool=False,
                       revision=None,
                       **kwargs) -> Union[dict, BaseGoogleDriveInstitutionsMetadata,
                                          List[Union[BaseGoogleDriveInstitutionsMetadata, dict]]]:
        if path.identifier is None:
            raise exceptions.MetadataError('{} not found'.format(str(path)), code=404)

        if path.is_dir:
            return await self._folder_metadata(path, raw=raw)

        return await self._file_metadata(path, revision=revision, raw=raw)

    async def revisions(self, path: GoogleDriveInstitutionsPath,  # type: ignore
                        **kwargs) -> List[GoogleDriveInstitutionsRevision]:
        """Returns list of revisions for the file at ``path``.

        Google Drive will not allow a user to view the revision list of a file if they only have
        view or commenting permissions.  It will return a 403 Unathorized.  If that happens, then
        we construct a recognizable dummy revision based off of the metadata of the current file
        version.

        Note: though we explicitly support the case where the revision list is empty, I have yet to
        see it in practice.  The current handling is based on historical behavior.

        :param GoogleDriveInstitutionsPath path: the path of the file to fetch revisions for
        :rtype: `list(GoogleDriveInstitutionsRevision)`
        :return: list of `GoogleDriveInstitutionsRevision` objects representing revisions of the file
        """
        if path.identifier is None:
            raise exceptions.NotFoundError(str(path))

        async with self.request(
            'GET',
            self.build_url('files', path.identifier, 'revisions'),
            expects=(200, 403, ),
            throws=exceptions.RevisionsError,
        ) as resp:
            data = await resp.json()
            has_revisions = resp.status == 200

        if has_revisions and data['revisions']:
            return [
                GoogleDriveInstitutionsRevision(revision)
                for revision in reversed(data['revisions'])
            ]

        # Use dummy ID if no revisions found
        metadata = await self.metadata(path, raw=True)
        revision = {
            'modifiedTime': metadata['modifiedTime'],  # type: ignore
            'id': metadata['modifiedTime'] + pd_settings.DRIVE_IGNORE_VERSION,  # type: ignore
        }
        return [GoogleDriveInstitutionsRevision(revision), ]

    async def create_folder(self,
                            path: WaterButlerPath,
                            folder_precheck: bool=True,
                            **kwargs) -> GoogleDriveInstitutionsFolderMetadata:
        GoogleDriveInstitutionsPath.validate_folder(path)

        if folder_precheck:
            if path.identifier:
                raise exceptions.FolderNamingConflict(path.name)

        async with self.request(
            'POST',
            self.build_url('files'),
            headers={
                'Content-Type': 'application/json',
            },
            data=json.dumps({
                'name': path.name,
                'parents': [{
                    'id': path.parent.identifier
                }],
                'mimeType': self.FOLDER_MIME_TYPE,
            }),
            expects=(200, ),
            throws=exceptions.CreateFolderError,
        ) as resp:
            return GoogleDriveInstitutionsFolderMetadata(await resp.json(), path)

    def path_from_metadata(self, parent_path, metadata):
        """ Unfortunately-named method, currently only used to get path name for zip archives. """
        return parent_path.child(metadata.export_name, _id=metadata.id, folder=metadata.is_folder)

    def _build_upload_url(self, *segments, **query):
        return provider.build_url(pd_settings.BASE_UPLOAD_URL, *segments, **query)

    def _serialize_revisions(self,
                        path: WaterButlerPath,
                        revisions: dict,
                        raw: bool=False) -> Union[BaseGoogleDriveInstitutionsMetadata, dict]:
        if raw:
            return revisions
        if revisions['mimeType'] == self.FOLDER_MIME_TYPE:
            return GoogleDriveInstitutionsFolderMetadata(revisions, path)
        return GoogleDriveInstitutionsFileMetadata(revisions, path)

    def _build_upload_metadata(self, folder_id: str, name: str) -> dict:
        return {
            'parents': [
                {
                    'id': folder_id,
                },
            ],
            'name': name,
        }

    async def _start_resumable_upload(self,
                                      created: bool,
                                      segments: Sequence[str],
                                      size,
                                      metadata: dict) -> str:
        async with self.request(
            'POST' if created else 'PUT',
            self._build_upload_url('files', *segments, uploadType='resumable'),
            headers={
                'Content-Type': 'application/json; charset=UTF-8',
                'X-Upload-Content-Length': str(size),
            },
            data=json.dumps(metadata),
            expects=(200, ),
            throws=exceptions.UploadError,
        ) as resp:
            location = furl.furl(resp.headers['Location'])
        return location

    async def _finish_resumable_upload(self, segments: Sequence[str], stream, location):
        async with self.request(
            'PUT',
            location,
            headers={'Content-Length': str(stream.size)},
            data=stream,
            expects=(200, ),
            throws=exceptions.UploadError,
        ) as resp:
            return await resp.json()

    async def _resolve_path_to_ids(self, path, start_at=None):
        """Takes a path and traverses the file tree (ha!) beginning at ``start_at``, looking for
        something that matches ``path``.  Returns a list of dicts for each part of the path, with
        ``name``, ``mimeType``, and ``id`` keys.
        """
        self.metrics.incr('called_resolve_path_to_ids')
        ret = start_at or [{
            'name': '',
            'mimeType': 'folder',
            'id': self.folder['id'],
        }]
        file_id = ret[0]['id']
        # parts is list of [path_part_name, is_folder]
        parts = [[parse.unquote(x), True] for x in path.strip('/').split('/')]

        if not path.endswith('/'):
            parts[-1][1] = False
        while parts:

            async with self.request(
                'GET',
                self.build_url('files', file_id, fields='parents(id)'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            ) as resp:
                parents_id = await resp.json()

            current_part = parts.pop(0)
            part_name, part_is_folder = current_part[0], current_part[1]
            name, ext = os.path.splitext(part_name)
            if not part_is_folder and ext in ('.gdoc', '.gdraw', '.gslides', '.gsheet'):
                gd_ext = utils.get_mimetype_from_ext(ext)
                query = "name = '{}' " \
                        "and trashed = false " \
                        "and mimeType = '{}'" \
                        "{} in parents".format(clean_query(name), gd_ext, parents_id)
            else:
                query = "name = '{}' " \
                        "and trashed = false " \
                        "and mimeType != 'application/vnd.google-apps.form' " \
                        "and mimeType != 'application/vnd.google-apps.map' " \
                        "and mimeType != 'application/vnd.google-apps.document' " \
                        "and mimeType != 'application/vnd.google-apps.drawing' " \
                        "and mimeType != 'application/vnd.google-apps.presentation' " \
                        "and mimeType != 'application/vnd.google-apps.spreadsheet' " \
                        "and mimeType {} '{}'" \
                        "{} in parents".format(
                            clean_query(part_name),
                            '=' if part_is_folder else '!=',
                            self.FOLDER_MIME_TYPE,
                            parents_id
                        )
            async with self.request(
                'GET',
                self.build_url('files', file_id, q=query, fields='id'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            ) as resp:
                data = await resp.json()

            try:
                file_id = data['files'][0]['id']
            except (KeyError, IndexError):
                if parts:
                    # if we can't find an intermediate path part, that's an error
                    raise exceptions.MetadataError('{} not found'.format(str(path)),
                                                   code=HTTPStatus.NOT_FOUND)
                return ret + [{
                    'id': None,
                    'title': part_name,
                    'mimeType': 'folder' if part_is_folder else '',
                }]

            async with self.request(
                'GET',
                self.build_url('files', file_id, fields='id,name,mimeType'),
                expects=(200, ),
                throws=exceptions.MetadataError,
            ) as resp:
                ret.append(await resp.json())
        return ret

    async def _handle_docs_versioning(self, path: GoogleDriveInstitutionsPath, revisions: dict, raw: bool=True):
        """Sends an extra request to GDrive to fetch revision information for Google Docs. Needed
        because Google Docs use a different versioning system from regular files.

        I've been unable to replicate the case where revisions_data['revisions'] is None.  I'm leaving
        it in for now and adding a metric to see if we ever actually encounter this case.  If not,
        we should probably remove it to simplify this method.

        This method does not handle the case of read-only google docs, which will return a 403.
        Other methods should check the ``userPermission.role`` field of the file metadata before
        calling this.  If the value of that field is ``"reader"`` or ``"commenter"``, this method
        will error.

        :param GoogleDrivePath path: the path of the google doc to get version information for
        :param dict item: a raw response object from the GDrive file metadata endpoint
        :param bool raw: should we return the raw response object from the GDrive API?
        :rtype: GoogleDriveFileMetadata
        :rtype: dict
        :return: a metadata for the googledoc or the raw response object from the GDrive API
        """
        async with self.request(
            'GET',
            self.build_url('files', revisions['id'], 'revisions'),
            expects=(200, ),
            throws=exceptions.RevisionsError,
        ) as resp:
            revisions_data = await resp.json()
            has_revisions = revisions_data['revisions'] is not None

        # Revisions are not available for some sharing configurations. If revisions list is empty,
        # use the etag of the file plus a sentinel string as a dummy revision ID.
        self.metrics.add('handle_docs_versioning.empty_revision_list', not has_revisions)
        if has_revisions:
            revisions['version'] = revisions_data['revisions'][-1]['id']
        else:
            # If there are no revisions use etag as vid
            revisions['version'] = revisions['etag'] + pd_settings.DRIVE_IGNORE_VERSION

        return self._serialize_revisions(path, revisions, raw=raw)

    async def _folder_metadata(self,
                               path: WaterButlerPath,
                               raw: bool=False) -> List[Union[BaseGoogleDriveInstitutionsMetadata, dict]]:
        query = self._build_query(path.identifier)
        built_url = self.build_url('files', q=query, alt='json', pageSize=1000, # v2:maxResults
                                    fields='nextPageToken,files')
        full_resp = []
        while built_url:
            async with self.request(
                'GET',
                built_url,
                expects=(200, ),
                throws=exceptions.MetadataError,
            ) as resp:
                resp_json = await resp.json()
                full_resp.extend([
                    self._serialize_revisions(path.child(item['files']), item, raw=raw)
                    for item in resp_json['files']
                ])
                nextPageToken = resp_json['nextPageToken']
                if nextPageToken:
                    built_url = self.build_url('files', q=query, alt='json', pageSize=1000,
                                                pageToken=nextPageToken, fields='nextPageToken,files')
                else:
                    built_url = None
        return full_resp

    async def _file_metadata(self,
                             path: GoogleDriveInstitutionsPath,
                             revision: str=None,
                             raw: bool=False) -> Union[dict, BaseGoogleDriveInstitutionsMetadata]:
        """ Returns metadata for the file identified by `path`.  If the `revision` arg is set,
        will attempt to return metadata for the given revision of the file.  If the revision does
        not exist, ``_file_metadata`` will throw a 404.

        This method used to error with a 500 when metadata was requested for a file that the
        authorizing user only had view or commenting permissions for.  The GDrive revisions
        endpoint returns a 403, which was not being handled.  WB postpends a sentinel value to the
        revisions for these files.  If a revision ending with this sentinel value is detected, this
        method will return metadata for the latest revision of the file.  If a revision NOT ending
        in the sentinel value is requested for a read-only file, this method will return a 404 Not
        Found instead.

        Metrics:

        ``_file_metadata.got_revision``: did this request include a revision parameter?

        ``_file_metadata.revision_is_valid``: if a revision was given, was it valid? A revision is
        "valid" if it doesn't end with our sentinal string (`settings.DRIVE_IGNORE_VERSION`).

        ``_file_metadata.user_role``: What role did the user possess? Helps identify other roles
        for which revision information isn't available.

        :param GoogleDriveInstitutionsPath path: the path of the file whose metadata is being requested
        :param str revision: a string representing the ID of the revision (default: `None`)
        :param bool raw: should we return the raw response object from the GDrive API?
        :rtype: GoogleDriveInstitutionsFileMetadata
        :rtype: dict
        :return: a metadata for the googledoc or the raw response object from the GDrive API
        """

        self.metrics.add('_file_metadata.got_revision', revision is not None)

        valid_revision = revision and not revision.endswith(pd_settings.DRIVE_IGNORE_VERSION)
        if revision:
            self.metrics.add('_file_metadata.revision_is_valid', valid_revision)

        if revision and valid_revision:
            url = self.build_url('files', path.identifier, 'revisions', revision,
                                fields='id,mimeType,modifiedTime,md5Checksum,size,exportLinks')
        else:
            url = self.build_url('files', path.identifier,
                                fields='id,name,version,size,modifiedTime,createdTime,mimeType,' \
                                    'md5Checksum,originalFilename,exportLinks,ownedByMe,capabilities(canEdit)')

        async with self.request(
            'GET',
            url,
            expects=(200, 403, 404, ),
            throws=exceptions.MetadataError,
        ) as resp:
            try:
                data = await resp.json()
            except:  # some 404s return a string instead of json
                data = await resp.read()

        if resp.status != 200:
            raise exceptions.NotFoundError(path)

        if revision and valid_revision:
            return GoogleDriveInstitutionsFileRevisionMetadata(data, path)

        self.metrics.add('_file_metadata.user_role', 'ownedByMe:' + data['ownedByMe'] + ', canEdit:' + data['canEdit'])
        can_access_revisions = data['ownedByMe'] or data['canEdit']
        if utils.is_docs_file(data):
            if can_access_revisions:
                return await self._handle_docs_versioning(path, data, raw=raw)
            else:
                # Revisions are not available for some sharing configurations. If revisions list is
                # empty, use the modifiedTime of the file plus a sentinel string as a dummy revision ID.
                data['version'] = data['modifiedTime'] + pd_settings.DRIVE_IGNORE_VERSION

        return data if raw else GoogleDriveInstitutionsFileMetadata(data, path)

    async def _delete_folder_contents(self, path: WaterButlerPath) -> None:
        """Given a WaterButlerPath, delete all contents of folder

        :param WaterButlerPath path: Folder to be emptied
        :rtype: None
        :raises: :class:`waterbutler.core.exceptions.NotFoundError`
        :raises: :class:`waterbutler.core.exceptions.MetadataError`
        :raises: :class:`waterbutler.core.exceptions.DeleteError`
        """
        file_id = path.identifier
        if not file_id:
            raise exceptions.NotFoundError(str(path))
        resp = await self.make_request(
            'GET',
            self.build_url('files',
                           q="'{}' in parents".format(file_id),
                           fields='files(id)'),
            expects=(200, ),
            throws=exceptions.MetadataError)

        try:
            child_ids = (await resp.json())['files']
        except (KeyError, IndexError):
            raise exceptions.MetadataError('{} not found'.format(str(path)),
                                           code=HTTPStatus.NOT_FOUND)

        for child in child_ids:
            await self.make_request(
                'DELETE',
                self.build_url('files', child['id']),
                data=json.dumps({'labels': {'trashed': 'true'}}),
                headers={'Content-Type': 'application/json'},
                expects=(200, ),
                throws=exceptions.DeleteError)