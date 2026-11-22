package r2workspace

// These Python programs run inside a sandbox. They use only the standard
// library so the same archive format and transfer path can work with Cube and
// another provider. Signed URLs are temporary credentials: errors deliberately
// omit request URLs, and the manager never logs the command text.

const snapshotScript = `
import base64, gzip, hashlib, json, os, stat, sys, tarfile
root, dest, limit = sys.argv[1], sys.argv[2], int(sys.argv[3])
try:
    count, total = 0, 0
    with open(dest, 'wb') as raw:
        with gzip.GzipFile(filename='', mode='wb', fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode='w') as archive:
                for parent, dirs, files in os.walk(root, followlinks=False):
                    dirs.sort(); files.sort()
                    for name in dirs + files:
                        full = os.path.join(parent, name)
                        entry = os.lstat(full)
                        mode = entry.st_mode
                        if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
                            raise ValueError('workspace contains an unsupported file type')
                        count += 1
                        if count > 100000: raise ValueError('too many workspace entries')
                        if stat.S_ISREG(mode): total += entry.st_size
                        if total > 1024 * 1024 * 1024: raise ValueError('workspace expands too large')
                        archive.add(full, arcname=os.path.relpath(full, root), recursive=False)
    size = os.path.getsize(dest)
    if size > limit:
        raise ValueError('workspace archive exceeds size limit')
    md5, sha = hashlib.md5(), hashlib.sha256()
    with open(dest, 'rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            md5.update(chunk); sha.update(chunk)
    print(json.dumps({'path': dest, 'size': size,
                      'md5': base64.b64encode(md5.digest()).decode(),
                      'sha256': sha.hexdigest()}))
except Exception as exc:
    if os.path.exists(dest): os.unlink(dest)
    print(type(exc).__name__ + ': ' + str(exc), file=sys.stderr)
    sys.exit(1)
`

const removeArchiveScript = `import os,sys; os.unlink(sys.argv[1])`

const uploadScript = `
import json, os, sys, urllib.error, urllib.request
url, archive, headers, expected = sys.argv[1], sys.argv[2], json.loads(sys.argv[3]), int(sys.argv[4])
try:
    size = os.path.getsize(archive)
    if size != expected:
        raise ValueError('archive changed before upload')
    headers['Content-Length'] = str(size)
    with open(archive, 'rb') as source:
        request = urllib.request.Request(url, data=source, headers=headers, method='PUT')
        with urllib.request.urlopen(request, timeout=240) as response:
            if response.status < 200 or response.status >= 300:
                raise ValueError('object store rejected upload')
except urllib.error.HTTPError as exc:
    print('object store rejected upload: HTTP ' + str(exc.code), file=sys.stderr)
    sys.exit(1)
except Exception as exc:
    print('upload failed: ' + type(exc).__name__, file=sys.stderr)
    sys.exit(1)
finally:
    if os.path.exists(archive): os.unlink(archive)
`

const restoreScript = `
import hashlib, os, pathlib, shutil, sys, tarfile, tempfile, urllib.error, urllib.request
url, expected_sha, expected_size, root = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
try:
    temp = tempfile.NamedTemporaryFile(prefix='polyagent-restore-', suffix='.tar.gz', delete=False)
    size, sha = 0, hashlib.sha256()
    try:
        with urllib.request.urlopen(url, timeout=240) as response, temp:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk: break
                size += len(chunk)
                if size > expected_size: raise ValueError('archive exceeds committed size')
                sha.update(chunk); temp.write(chunk)
        if size != expected_size or sha.hexdigest() != expected_sha:
            raise ValueError('archive checksum mismatch')
        with tarfile.open(temp.name, 'r:gz') as archive:
            members = archive.getmembers()
            if len(members) > 100000: raise ValueError('too many archive entries')
            total = 0
            for member in members:
                p = pathlib.PurePosixPath(member.name)
                if p.is_absolute() or '..' in p.parts or not (member.isdir() or member.isfile()):
                    raise ValueError('unsafe archive entry')
                total += member.size
                if total > 1024 * 1024 * 1024: raise ValueError('archive expands too large')
            for member in members:
                dest = os.path.join(root, member.name)
                if member.isdir():
                    os.makedirs(dest, exist_ok=True)
                else:
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    with archive.extractfile(member) as source, open(dest, 'wb') as target:
                        shutil.copyfileobj(source, target)
                    os.chmod(dest, member.mode & 0o777)
    finally:
        os.unlink(temp.name)
except urllib.error.HTTPError as exc:
    print('object store rejected download: HTTP ' + str(exc.code), file=sys.stderr)
    sys.exit(1)
except Exception as exc:
    print('restore failed: ' + type(exc).__name__, file=sys.stderr)
    sys.exit(1)
`
